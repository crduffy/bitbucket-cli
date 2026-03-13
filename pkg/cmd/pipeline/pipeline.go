package pipeline

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/avivsinai/bitbucket-cli/internal/config"
	"github.com/avivsinai/bitbucket-cli/pkg/bbcloud"
	"github.com/avivsinai/bitbucket-cli/pkg/cmdutil"
	"github.com/avivsinai/bitbucket-cli/pkg/iostreams"
)

// NewCmdPipeline interacts with Bitbucket Cloud pipelines.
func NewCmdPipeline(f *cmdutil.Factory) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pipeline",
		Short: "Run and inspect Bitbucket Cloud pipelines",
		Long:  "Interact with Bitbucket Cloud Pipelines. Commands are no-ops for Data Center contexts.",
	}

	cmd.AddCommand(newRunCmd(f))
	cmd.AddCommand(newListCmd(f))
	cmd.AddCommand(newViewCmd(f))
	cmd.AddCommand(newLogsCmd(f))
	cmd.AddCommand(newStepsCmd(f))
	cmd.AddCommand(newStopCmd(f))
	cmd.AddCommand(newWatchCmd(f))
	cmd.AddCommand(newOpenCmd(f))

	return cmd
}

// ---------------------------------------------------------------------------
// Shared option types
// ---------------------------------------------------------------------------

type baseOptions struct {
	Workspace string
	Repo      string
}

type runOptions struct {
	baseOptions
	Ref       string
	Variables []string
}

type listOptions struct {
	baseOptions
	Limit   int
	State   string
	Result  string
	Ref     string
	Creator string
}

type viewOptions struct {
	baseOptions
	Identifier string // UUID or build number
	Web        bool
}

type logsOptions struct {
	baseOptions
	Identifier string // UUID or build number
	Step       string
	Failed     bool
	All        bool
}

type stepsOptions struct {
	baseOptions
	Identifier string
}

type stopOptions struct {
	baseOptions
	Identifier string
}

type watchOptions struct {
	baseOptions
	Identifier    string
	Interval      time.Duration
	MaxInterval   time.Duration
	Timeout       time.Duration
	LogsOnFailure bool
}

type openOptions struct {
	baseOptions
	Identifier string
}

// ---------------------------------------------------------------------------
// Color helpers
// ---------------------------------------------------------------------------

const (
	ansiReset  = "\033[0m"
	ansiRed    = "\033[31m"
	ansiGreen  = "\033[32m"
	ansiYellow = "\033[33m"
	ansiCyan   = "\033[36m"
	ansiBold   = "\033[1m"
)

func colorize(s string, color string, enabled bool) string {
	if !enabled {
		return s
	}
	return color + s + ansiReset
}

func stateColor(state, result string, colorEnabled bool) string {
	if !colorEnabled {
		if result != "" {
			return result
		}
		return state
	}
	switch strings.ToUpper(result) {
	case "SUCCESSFUL":
		return ansiGreen + "SUCCESSFUL" + ansiReset
	case "FAILED", "ERROR":
		return ansiRed + result + ansiReset
	case "STOPPED":
		return ansiYellow + "STOPPED" + ansiReset
	}
	switch strings.ToUpper(state) {
	case "PENDING":
		return ansiCyan + "PENDING" + ansiReset
	case "RUNNING", "BUILDING":
		return ansiYellow + state + ansiReset
	case "COMPLETED":
		return ansiGreen + "COMPLETED" + ansiReset
	}
	if result != "" {
		return result
	}
	return state
}

func stepIcon(result string, colorEnabled bool) string {
	switch strings.ToUpper(result) {
	case "SUCCESSFUL":
		return colorize("✓", ansiGreen, colorEnabled)
	case "FAILED", "ERROR":
		return colorize("✗", ansiRed, colorEnabled)
	case "STOPPED":
		return colorize("■", ansiYellow, colorEnabled)
	default:
		return "·"
	}
}

func formatDuration(seconds int) string {
	if seconds <= 0 {
		return "—"
	}
	d := time.Duration(seconds) * time.Second
	if d < time.Minute {
		return fmt.Sprintf("%ds", seconds)
	}
	m := int(d.Minutes())
	s := seconds - m*60
	return fmt.Sprintf("%dm %02ds", m, s)
}

func creatorName(p *bbcloud.Pipeline) string {
	if p.Creator != nil {
		if p.Creator.Display != "" {
			return p.Creator.Display
		}
		return p.Creator.Username
	}
	return ""
}

// ---------------------------------------------------------------------------
// Command constructors
// ---------------------------------------------------------------------------

func newRunCmd(f *cmdutil.Factory) *cobra.Command {
	opts := &runOptions{}
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Trigger a new pipeline run",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPipelineRun(cmd, f, opts)
		},
	}

	cmd.Flags().StringVar(&opts.Workspace, "workspace", "", "Bitbucket Cloud workspace override")
	cmd.Flags().StringVar(&opts.Repo, "repo", "", "Repository slug override")
	cmd.Flags().StringVar(&opts.Ref, "ref", "main", "Git ref to run the pipeline on")
	cmd.Flags().StringSliceVar(&opts.Variables, "var", nil, "Pipeline variable in KEY=VALUE form (repeatable)")

	return cmd
}

func newListCmd(f *cmdutil.Factory) *cobra.Command {
	opts := &listOptions{Limit: 20}
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List recent pipeline runs",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPipelineList(cmd, f, opts)
		},
	}

	cmd.Flags().StringVar(&opts.Workspace, "workspace", "", "Bitbucket Cloud workspace override")
	cmd.Flags().StringVar(&opts.Repo, "repo", "", "Repository slug override")
	cmd.Flags().IntVar(&opts.Limit, "limit", opts.Limit, "Maximum pipelines to display")
	cmd.Flags().StringVar(&opts.State, "state", "", "Filter by state (PENDING, BUILDING, COMPLETED)")
	cmd.Flags().StringVar(&opts.Result, "result", "", "Filter by result (SUCCESSFUL, FAILED, ERROR, STOPPED)")
	cmd.Flags().StringVar(&opts.Ref, "ref", "", "Filter by target branch")
	cmd.Flags().StringVar(&opts.Creator, "creator", "", "Filter by creator username")

	return cmd
}

func newViewCmd(f *cmdutil.Factory) *cobra.Command {
	opts := &viewOptions{}
	cmd := &cobra.Command{
		Use:   "view <id>",
		Short: "Show details for a pipeline run",
		Long:  "Show details for a pipeline run. The <id> can be either a build number (e.g., 10) or a UUID.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.Identifier = args[0]
			return runPipelineView(cmd, f, opts)
		},
	}

	cmd.Flags().StringVar(&opts.Workspace, "workspace", "", "Bitbucket Cloud workspace override")
	cmd.Flags().StringVar(&opts.Repo, "repo", "", "Repository slug override")
	cmd.Flags().BoolVar(&opts.Web, "web", false, "Open the pipeline in the browser")

	return cmd
}

func newLogsCmd(f *cmdutil.Factory) *cobra.Command {
	opts := &logsOptions{}
	cmd := &cobra.Command{
		Use:   "logs <id>",
		Short: "Fetch logs for a pipeline run",
		Long: `Fetch logs for a pipeline run. The <id> can be either a build number (e.g., 10) or a UUID.

By default, shows logs for the last step. Use --failed to show logs for the
first failed step, --step to select by name/index/UUID, or --all to show all.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.Identifier = args[0]
			return runPipelineLogs(cmd, f, opts)
		},
	}

	cmd.Flags().StringVar(&opts.Workspace, "workspace", "", "Bitbucket Cloud workspace override")
	cmd.Flags().StringVar(&opts.Repo, "repo", "", "Repository slug override")
	cmd.Flags().StringVar(&opts.Step, "step", "", "Step to fetch logs for (name, 1-based index, or UUID)")
	cmd.Flags().BoolVar(&opts.Failed, "failed", false, "Show logs for the first failed step")
	cmd.Flags().BoolVar(&opts.All, "all", false, "Show logs for all steps")

	return cmd
}

func newStepsCmd(f *cmdutil.Factory) *cobra.Command {
	opts := &stepsOptions{}
	cmd := &cobra.Command{
		Use:   "steps <id>",
		Short: "List steps for a pipeline run",
		Long:  "List steps for a pipeline run with detailed timing and status.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.Identifier = args[0]
			return runPipelineSteps(cmd, f, opts)
		},
	}

	cmd.Flags().StringVar(&opts.Workspace, "workspace", "", "Bitbucket Cloud workspace override")
	cmd.Flags().StringVar(&opts.Repo, "repo", "", "Repository slug override")

	return cmd
}

func newStopCmd(f *cmdutil.Factory) *cobra.Command {
	opts := &stopOptions{}
	cmd := &cobra.Command{
		Use:   "stop <id>",
		Short: "Stop a running pipeline",
		Long:  "Stop a running pipeline. The <id> can be either a build number (e.g., 10) or a UUID.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.Identifier = args[0]
			return runPipelineStop(cmd, f, opts)
		},
	}

	cmd.Flags().StringVar(&opts.Workspace, "workspace", "", "Bitbucket Cloud workspace override")
	cmd.Flags().StringVar(&opts.Repo, "repo", "", "Repository slug override")

	return cmd
}

func newWatchCmd(f *cmdutil.Factory) *cobra.Command {
	opts := &watchOptions{
		Interval:    5 * time.Second,
		MaxInterval: 30 * time.Second,
	}
	cmd := &cobra.Command{
		Use:   "watch <id>",
		Short: "Watch a pipeline until it completes",
		Long: `Watch a pipeline execution in real-time, polling for status updates until
the pipeline completes. Optionally dump failed step logs on completion.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.Identifier = args[0]
			return runPipelineWatch(cmd, f, opts)
		},
	}

	cmd.Flags().StringVar(&opts.Workspace, "workspace", "", "Bitbucket Cloud workspace override")
	cmd.Flags().StringVar(&opts.Repo, "repo", "", "Repository slug override")
	cmd.Flags().DurationVar(&opts.Interval, "interval", opts.Interval, "Initial poll interval")
	cmd.Flags().DurationVar(&opts.MaxInterval, "max-interval", opts.MaxInterval, "Maximum poll interval")
	cmd.Flags().DurationVar(&opts.Timeout, "timeout", 0, "Overall timeout (0 = no timeout)")
	cmd.Flags().BoolVar(&opts.LogsOnFailure, "logs-on-failure", false, "Show failed step logs when pipeline completes with failure")

	return cmd
}

func newOpenCmd(f *cmdutil.Factory) *cobra.Command {
	opts := &openOptions{}
	cmd := &cobra.Command{
		Use:   "open <id>",
		Short: "Open a pipeline in the browser",
		Long:  "Open a pipeline run in the default web browser.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts.Identifier = args[0]
			return runPipelineOpen(cmd, f, opts)
		},
	}

	cmd.Flags().StringVar(&opts.Workspace, "workspace", "", "Bitbucket Cloud workspace override")
	cmd.Flags().StringVar(&opts.Repo, "repo", "", "Repository slug override")

	return cmd
}

// ---------------------------------------------------------------------------
// Command implementations
// ---------------------------------------------------------------------------

func runPipelineRun(cmd *cobra.Command, f *cmdutil.Factory, opts *runOptions) error {
	ios, err := f.Streams()
	if err != nil {
		return err
	}

	workspace, repo, host, err := resolveCloudRepo(cmd, f, opts.Workspace, opts.Repo)
	if err != nil {
		return err
	}

	client, err := cmdutil.NewCloudClient(host)
	if err != nil {
		return err
	}

	vars := make(map[string]string)
	for _, v := range opts.Variables {
		parts := strings.SplitN(v, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid variable %q, expected KEY=VALUE", v)
		}
		vars[strings.TrimSpace(parts[0])] = parts[1]
	}

	ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
	defer cancel()

	pipeline, err := client.TriggerPipeline(ctx, workspace, repo, bbcloud.TriggerPipelineInput{
		Ref:       opts.Ref,
		Variables: vars,
	})
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(ios.Out, "Triggered pipeline #%d on %s/%s (ref: %s)\n",
		pipeline.BuildNumber, workspace, repo, opts.Ref)
	return err
}

func runPipelineList(cmd *cobra.Command, f *cmdutil.Factory, opts *listOptions) error {
	ios, err := f.Streams()
	if err != nil {
		return err
	}

	workspace, repo, host, err := resolveCloudRepo(cmd, f, opts.Workspace, opts.Repo)
	if err != nil {
		return err
	}

	client, err := cmdutil.NewCloudClient(host)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
	defer cancel()

	// Fetch more than needed when client-side filtering is active.
	fetchLimit := opts.Limit
	hasFilter := opts.State != "" || opts.Result != "" || opts.Ref != "" || opts.Creator != ""
	if hasFilter {
		fetchLimit = opts.Limit * 3
		if fetchLimit < 50 {
			fetchLimit = 50
		}
	}

	pipelines, err := client.ListPipelines(ctx, workspace, repo, fetchLimit)
	if err != nil {
		return err
	}

	// Client-side filtering.
	if hasFilter {
		pipelines = filterPipelines(pipelines, opts)
	}

	// Enforce display limit after filtering.
	if opts.Limit > 0 && len(pipelines) > opts.Limit {
		pipelines = pipelines[:opts.Limit]
	}

	payload := map[string]any{
		"workspace": workspace,
		"repo":      repo,
		"pipelines": pipelines,
	}

	return cmdutil.WriteOutput(cmd, ios.Out, payload, func() error {
		if len(pipelines) == 0 {
			_, err := fmt.Fprintln(ios.Out, "No pipelines found.")
			return err
		}

		colorEnabled := ios.ColorEnabled()

		for _, p := range pipelines {
			created := ""
			if p.CreatedOn != "" {
				if t, err := time.Parse(time.RFC3339Nano, p.CreatedOn); err == nil {
					created = t.Local().Format("2006-01-02 15:04")
				}
			}
			dur := formatDuration(p.DurationInSeconds)
			status := stateColor(p.State.Name, p.State.Result.Name, colorEnabled)
			creator := creatorName(&p)

			if _, err := fmt.Fprintf(ios.Out, "#%-4d  %-12s  %-14s  %-6s  %-16s  %s\n",
				p.BuildNumber, status, p.Target.Ref.Name, dur, created, creator); err != nil {
				return err
			}
		}
		return nil
	})
}

func filterPipelines(pipelines []bbcloud.Pipeline, opts *listOptions) []bbcloud.Pipeline {
	var filtered []bbcloud.Pipeline
	for _, p := range pipelines {
		if opts.State != "" && !strings.EqualFold(p.State.Name, opts.State) {
			continue
		}
		if opts.Result != "" && !strings.EqualFold(p.State.Result.Name, opts.Result) {
			continue
		}
		if opts.Ref != "" && !strings.EqualFold(p.Target.Ref.Name, opts.Ref) {
			continue
		}
		if opts.Creator != "" {
			name := ""
			if p.Creator != nil {
				name = p.Creator.Display
				if name == "" {
					name = p.Creator.Username
				}
			}
			if !strings.EqualFold(name, opts.Creator) && (p.Creator == nil || !strings.EqualFold(p.Creator.Username, opts.Creator)) {
				continue
			}
		}
		filtered = append(filtered, p)
	}
	return filtered
}

func runPipelineView(cmd *cobra.Command, f *cmdutil.Factory, opts *viewOptions) error {
	ios, err := f.Streams()
	if err != nil {
		return err
	}

	workspace, repo, host, err := resolveCloudRepo(cmd, f, opts.Workspace, opts.Repo)
	if err != nil {
		return err
	}

	client, err := cmdutil.NewCloudClient(host)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
	defer cancel()

	pipeline, err := resolvePipeline(ctx, client, workspace, repo, opts.Identifier)
	if err != nil {
		return err
	}

	if opts.Web {
		url := pipelineWebURL(pipeline, workspace, repo)
		if url == "" {
			return fmt.Errorf("no web URL available for pipeline #%d", pipeline.BuildNumber)
		}
		browser := f.BrowserOpener()
		return browser.Open(url)
	}

	steps, err := client.ListPipelineSteps(ctx, workspace, repo, pipeline.UUID)
	if err != nil {
		return err
	}

	payload := map[string]any{
		"pipeline": pipeline,
		"steps":    steps,
	}

	return cmdutil.WriteOutput(cmd, ios.Out, payload, func() error {
		return renderPipelineDetail(ios, pipeline, steps)
	})
}

func renderPipelineDetail(ios *iostreams.IOStreams, pipeline *bbcloud.Pipeline, steps []bbcloud.PipelineStep) error {
	colorEnabled := ios.ColorEnabled()
	w := ios.Out

	status := stateColor(pipeline.State.Name, pipeline.State.Result.Name, colorEnabled)
	title := fmt.Sprintf("Pipeline #%d", pipeline.BuildNumber)
	if colorEnabled {
		title = ansiBold + title + ansiReset
	}

	if _, err := fmt.Fprintf(w, "%s  %s\n", title, status); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(w, "Branch:    %s\n", pipeline.Target.Ref.Name); err != nil {
		return err
	}

	if creator := creatorName(pipeline); creator != "" {
		if _, err := fmt.Fprintf(w, "Creator:   %s\n", creator); err != nil {
			return err
		}
	}

	if pipeline.CreatedOn != "" {
		if t, err := time.Parse(time.RFC3339Nano, pipeline.CreatedOn); err == nil {
			if _, err := fmt.Fprintf(w, "Created:   %s\n", t.Local().Format("2006-01-02 15:04:05")); err != nil {
				return err
			}
		}
	}

	if _, err := fmt.Fprintf(w, "Duration:  %s\n", formatDuration(pipeline.DurationInSeconds)); err != nil {
		return err
	}

	if url := pipelineWebURL(pipeline, "", ""); url != "" {
		if _, err := fmt.Fprintf(w, "URL:       %s\n", url); err != nil {
			return err
		}
	}

	if len(steps) > 0 {
		if _, err := fmt.Fprintf(w, "\nSteps:\n"); err != nil {
			return err
		}
		for _, step := range steps {
			icon := stepIcon(step.State.Result.Name, colorEnabled)
			dur := formatDuration(step.DurationInSeconds)
			resultText := stateColor(step.State.Name, step.State.Result.Name, colorEnabled)

			line := fmt.Sprintf("  %s %-20s %-14s %s", icon, step.Name, resultText, dur)

			// Show the failing command if available.
			if strings.EqualFold(step.State.Result.Name, "FAILED") || strings.EqualFold(step.State.Result.Name, "ERROR") {
				if failCmd := findFailedCommand(step); failCmd != "" {
					line += fmt.Sprintf("    <- %s", failCmd)
				}
			}

			if _, err := fmt.Fprintln(w, line); err != nil {
				return err
			}
		}
	}
	return nil
}

func findFailedCommand(step bbcloud.PipelineStep) string {
	for _, cmd := range step.ScriptCommands {
		if cmd.ExitCode != nil && *cmd.ExitCode != 0 {
			name := cmd.Name
			if name == "" {
				name = cmd.Command
			}
			return fmt.Sprintf("exit %d in %q", *cmd.ExitCode, name)
		}
	}
	for _, cmd := range step.SetupCommands {
		if cmd.ExitCode != nil && *cmd.ExitCode != 0 {
			name := cmd.Name
			if name == "" {
				name = cmd.Command
			}
			return fmt.Sprintf("exit %d in %q (setup)", *cmd.ExitCode, name)
		}
	}
	return ""
}

func runPipelineLogs(cmd *cobra.Command, f *cmdutil.Factory, opts *logsOptions) error {
	ios, err := f.Streams()
	if err != nil {
		return err
	}

	workspace, repo, host, err := resolveCloudRepo(cmd, f, opts.Workspace, opts.Repo)
	if err != nil {
		return err
	}

	client, err := cmdutil.NewCloudClient(host)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
	defer cancel()

	pipeline, err := resolvePipeline(ctx, client, workspace, repo, opts.Identifier)
	if err != nil {
		return err
	}

	steps, err := client.ListPipelineSteps(ctx, workspace, repo, pipeline.UUID)
	if err != nil {
		return err
	}
	if len(steps) == 0 {
		return fmt.Errorf("pipeline #%d has no steps yet", pipeline.BuildNumber)
	}

	// Determine which steps to fetch logs for.
	var targetSteps []bbcloud.PipelineStep

	switch {
	case opts.All:
		targetSteps = steps

	case opts.Failed:
		for _, s := range steps {
			if strings.EqualFold(s.State.Result.Name, "FAILED") || strings.EqualFold(s.State.Result.Name, "ERROR") {
				targetSteps = append(targetSteps, s)
				break
			}
		}
		if len(targetSteps) == 0 {
			return fmt.Errorf("no failed steps found in pipeline #%d", pipeline.BuildNumber)
		}

	case opts.Step != "":
		step, err := resolveStep(steps, opts.Step)
		if err != nil {
			return err
		}
		targetSteps = []bbcloud.PipelineStep{*step}

	default:
		// Default: last step.
		targetSteps = []bbcloud.PipelineStep{steps[len(steps)-1]}
	}

	colorEnabled := ios.ColorEnabled()
	multiStep := len(targetSteps) > 1

	for i, step := range targetSteps {
		if multiStep {
			header := fmt.Sprintf("=== Step: %s ===", step.Name)
			if colorEnabled {
				header = ansiBold + header + ansiReset
			}
			if _, err := fmt.Fprintln(ios.Out, header); err != nil {
				return err
			}
		}

		logs, err := client.GetPipelineLogs(ctx, workspace, repo, pipeline.UUID, step.UUID)
		if err != nil {
			return fmt.Errorf("fetching logs for step %q: %w", step.Name, err)
		}

		if _, err := ios.Out.Write(logs); err != nil {
			return err
		}

		if multiStep && i < len(targetSteps)-1 {
			if _, err := fmt.Fprintln(ios.Out); err != nil {
				return err
			}
		}
	}
	return nil
}

// resolveStep finds a step by name, 1-based index, or UUID.
func resolveStep(steps []bbcloud.PipelineStep, identifier string) (*bbcloud.PipelineStep, error) {
	// Try as 1-based index.
	if idx, err := strconv.Atoi(identifier); err == nil {
		if idx < 1 || idx > len(steps) {
			return nil, fmt.Errorf("step index %d out of range (1-%d)", idx, len(steps))
		}
		return &steps[idx-1], nil
	}

	// Try by name (case-insensitive).
	for i := range steps {
		if strings.EqualFold(steps[i].Name, identifier) {
			return &steps[i], nil
		}
	}

	// Try by UUID.
	for i := range steps {
		if steps[i].UUID == identifier || steps[i].UUID == "{"+identifier+"}" {
			return &steps[i], nil
		}
	}

	names := make([]string, len(steps))
	for i, s := range steps {
		names[i] = fmt.Sprintf("  %d: %s", i+1, s.Name)
	}
	return nil, fmt.Errorf("step %q not found; available steps:\n%s", identifier, strings.Join(names, "\n"))
}

func runPipelineSteps(cmd *cobra.Command, f *cmdutil.Factory, opts *stepsOptions) error {
	ios, err := f.Streams()
	if err != nil {
		return err
	}

	workspace, repo, host, err := resolveCloudRepo(cmd, f, opts.Workspace, opts.Repo)
	if err != nil {
		return err
	}

	client, err := cmdutil.NewCloudClient(host)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
	defer cancel()

	pipeline, err := resolvePipeline(ctx, client, workspace, repo, opts.Identifier)
	if err != nil {
		return err
	}

	steps, err := client.ListPipelineSteps(ctx, workspace, repo, pipeline.UUID)
	if err != nil {
		return err
	}

	payload := map[string]any{
		"pipeline_uuid": pipeline.UUID,
		"build_number":  pipeline.BuildNumber,
		"steps":         steps,
	}

	return cmdutil.WriteOutput(cmd, ios.Out, payload, func() error {
		if len(steps) == 0 {
			_, err := fmt.Fprintf(ios.Out, "Pipeline #%d has no steps.\n", pipeline.BuildNumber)
			return err
		}

		colorEnabled := ios.ColorEnabled()

		if _, err := fmt.Fprintf(ios.Out, "Steps for pipeline #%d:\n\n", pipeline.BuildNumber); err != nil {
			return err
		}

		for i, step := range steps {
			icon := stepIcon(step.State.Result.Name, colorEnabled)
			dur := formatDuration(step.DurationInSeconds)
			status := stateColor(step.State.Name, step.State.Result.Name, colorEnabled)

			if _, err := fmt.Fprintf(ios.Out, "  %d. %s %-20s %-14s %s\n",
				i+1, icon, step.Name, status, dur); err != nil {
				return err
			}

			if step.Image != nil && step.Image.Name != "" {
				if _, err := fmt.Fprintf(ios.Out, "       image: %s\n", step.Image.Name); err != nil {
					return err
				}
			}

			if failCmd := findFailedCommand(step); failCmd != "" {
				msg := failCmd
				if colorEnabled {
					msg = ansiRed + msg + ansiReset
				}
				if _, err := fmt.Fprintf(ios.Out, "       error: %s\n", msg); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func runPipelineStop(cmd *cobra.Command, f *cmdutil.Factory, opts *stopOptions) error {
	ios, err := f.Streams()
	if err != nil {
		return err
	}

	workspace, repo, host, err := resolveCloudRepo(cmd, f, opts.Workspace, opts.Repo)
	if err != nil {
		return err
	}

	client, err := cmdutil.NewCloudClient(host)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
	defer cancel()

	pipeline, err := resolvePipeline(ctx, client, workspace, repo, opts.Identifier)
	if err != nil {
		return err
	}

	if err := client.StopPipeline(ctx, workspace, repo, pipeline.UUID); err != nil {
		return err
	}

	_, err = fmt.Fprintf(ios.Out, "Stopped pipeline #%d\n", pipeline.BuildNumber)
	return err
}

func runPipelineWatch(cmd *cobra.Command, f *cmdutil.Factory, opts *watchOptions) error {
	ios, err := f.Streams()
	if err != nil {
		return err
	}

	workspace, repo, host, err := resolveCloudRepo(cmd, f, opts.Workspace, opts.Repo)
	if err != nil {
		return err
	}

	client, err := cmdutil.NewCloudClient(host)
	if err != nil {
		return err
	}

	ctx := cmd.Context()
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	// Initial fetch to validate the pipeline exists.
	fetchCtx, fetchCancel := context.WithTimeout(ctx, 15*time.Second)
	pipeline, err := resolvePipeline(fetchCtx, client, workspace, repo, opts.Identifier)
	fetchCancel()
	if err != nil {
		return err
	}

	isTTY := ios.IsStdoutTTY()
	colorEnabled := ios.ColorEnabled()
	interval := opts.Interval
	consecutiveErrors := 0

	if isTTY {
		ios.StartAlternateScreenBuffer()
		defer ios.StopAlternateScreenBuffer()
	}

	for {
		fetchCtx, fetchCancel = context.WithTimeout(ctx, 15*time.Second)
		pipeline, err = client.GetPipeline(fetchCtx, workspace, repo, pipeline.UUID)
		if err != nil {
			fetchCancel()
			consecutiveErrors++
			if consecutiveErrors >= 3 {
				return fmt.Errorf("too many consecutive errors: %w", err)
			}
			fmt.Fprintf(ios.ErrOut, "Error polling: %v (retrying...)\n", err)
		} else {
			consecutiveErrors = 0
		}

		steps, stepErr := client.ListPipelineSteps(fetchCtx, workspace, repo, pipeline.UUID)
		fetchCancel()
		if stepErr != nil && err == nil {
			steps = nil // Non-fatal; render without steps.
		}

		// Render current state.
		if isTTY {
			ios.ClearScreen()
		}

		if err == nil {
			_ = renderPipelineDetail(ios, pipeline, steps)
			nextPoll := interval
			if colorEnabled {
				fmt.Fprintf(ios.Out, "\n%sPolling every %s... (Ctrl-C to stop)%s\n",
					ansiCyan, nextPoll.Round(time.Second), ansiReset)
			} else {
				fmt.Fprintf(ios.Out, "\nPolling every %s... (Ctrl-C to stop)\n",
					nextPoll.Round(time.Second))
			}
		}

		// Check terminal state.
		if isTerminalState(pipeline.State.Name) {
			if isTTY {
				ios.StopAlternateScreenBuffer()
			}
			// Re-render final state on main screen.
			_ = renderPipelineDetail(ios, pipeline, steps)

			if opts.LogsOnFailure && isFailedResult(pipeline.State.Result.Name) {
				fmt.Fprintln(ios.Out)
				return dumpFailedLogs(ctx, ios, client, workspace, repo, pipeline, steps)
			}

			if isFailedResult(pipeline.State.Result.Name) {
				return fmt.Errorf("pipeline #%d failed", pipeline.BuildNumber)
			}
			return nil
		}

		// Backoff with jitter.
		interval = nextInterval(interval, opts.MaxInterval)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

func runPipelineOpen(cmd *cobra.Command, f *cmdutil.Factory, opts *openOptions) error {
	workspace, repo, host, err := resolveCloudRepo(cmd, f, opts.Workspace, opts.Repo)
	if err != nil {
		return err
	}

	client, err := cmdutil.NewCloudClient(host)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(cmd.Context(), 15*time.Second)
	defer cancel()

	pipeline, err := resolvePipeline(ctx, client, workspace, repo, opts.Identifier)
	if err != nil {
		return err
	}

	url := pipelineWebURL(pipeline, workspace, repo)
	if url == "" {
		return fmt.Errorf("no web URL available for pipeline #%d", pipeline.BuildNumber)
	}

	browser := f.BrowserOpener()
	return browser.Open(url)
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

// resolvePipeline fetches a pipeline by build number or UUID.
func resolvePipeline(ctx context.Context, client *bbcloud.Client, workspace, repo, identifier string) (*bbcloud.Pipeline, error) {
	if buildNum, err := strconv.Atoi(strings.TrimPrefix(identifier, "#")); err == nil {
		pipeline, err := client.GetPipelineByBuildNumber(ctx, workspace, repo, buildNum)
		if err == nil {
			return pipeline, nil
		}
		// Only fall through to UUID lookup if the error looks like a not-found;
		// propagate auth, network, and other real errors immediately.
		if !strings.Contains(err.Error(), "404") {
			return nil, err
		}
	}
	return client.GetPipeline(ctx, workspace, repo, identifier)
}

func resolveCloudRepo(cmd *cobra.Command, f *cmdutil.Factory, workspaceOverride, repoOverride string) (string, string, *config.Host, error) {
	return cmdutil.ResolveCloudRepo(f, cmd, workspaceOverride, repoOverride)
}

func pipelineWebURL(p *bbcloud.Pipeline, workspace, repo string) string {
	if p.Links.HTML.Href != "" {
		return p.Links.HTML.Href
	}
	if workspace != "" && repo != "" {
		return fmt.Sprintf("https://bitbucket.org/%s/%s/addon/pipelines/home#!/results/%d",
			workspace, repo, p.BuildNumber)
	}
	return ""
}

func isTerminalState(state string) bool {
	switch strings.ToUpper(state) {
	case "COMPLETED", "HALTED", "ERROR":
		return true
	}
	return false
}

func isFailedResult(result string) bool {
	switch strings.ToUpper(result) {
	case "FAILED", "ERROR":
		return true
	}
	return false
}

func nextInterval(current, max time.Duration) time.Duration {
	next := time.Duration(float64(current) * 1.5)
	if next > max {
		next = max
	}
	// Add ±15% jitter.
	jitter := float64(next) * 0.15
	next = time.Duration(float64(next) + (rand.Float64()*2-1)*jitter)
	return next
}

func dumpFailedLogs(ctx context.Context, ios *iostreams.IOStreams, client *bbcloud.Client, workspace, repo string, pipeline *bbcloud.Pipeline, steps []bbcloud.PipelineStep) error {
	colorEnabled := ios.ColorEnabled()

	for _, step := range steps {
		if !strings.EqualFold(step.State.Result.Name, "FAILED") && !strings.EqualFold(step.State.Result.Name, "ERROR") {
			continue
		}

		header := fmt.Sprintf("=== Logs: %s (FAILED) ===", step.Name)
		if colorEnabled {
			header = ansiRed + header + ansiReset
		}
		fmt.Fprintln(ios.Out, header)

		fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		logs, err := client.GetPipelineLogs(fetchCtx, workspace, repo, pipeline.UUID, step.UUID)
		cancel()
		if err != nil {
			fmt.Fprintf(ios.ErrOut, "Error fetching logs for step %q: %v\n", step.Name, err)
			continue
		}

		ios.Out.Write(logs)
		fmt.Fprintln(ios.Out)
		break // Only first failed step.
	}
	return nil
}

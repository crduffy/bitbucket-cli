package pipeline

import (
	"testing"
	"time"

	"github.com/avivsinai/bitbucket-cli/pkg/bbcloud"
)

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		seconds int
		want    string
	}{
		{0, "—"},
		{-1, "—"},
		{5, "5s"},
		{59, "59s"},
		{60, "1m 00s"},
		{90, "1m 30s"},
		{154, "2m 34s"},
		{3600, "60m 00s"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := formatDuration(tt.seconds)
			if got != tt.want {
				t.Errorf("formatDuration(%d) = %q, want %q", tt.seconds, got, tt.want)
			}
		})
	}
}

func TestStateColor_NoColor(t *testing.T) {
	tests := []struct {
		state  string
		result string
		want   string
	}{
		{"COMPLETED", "SUCCESSFUL", "SUCCESSFUL"},
		{"COMPLETED", "FAILED", "FAILED"},
		{"PENDING", "", "PENDING"},
		{"RUNNING", "", "RUNNING"},
		{"COMPLETED", "", "COMPLETED"},
	}

	for _, tt := range tests {
		t.Run(tt.state+"_"+tt.result, func(t *testing.T) {
			got := stateColor(tt.state, tt.result, false)
			if got != tt.want {
				t.Errorf("stateColor(%q, %q, false) = %q, want %q", tt.state, tt.result, got, tt.want)
			}
		})
	}
}

func TestStepIcon_NoColor(t *testing.T) {
	tests := []struct {
		result string
		want   string
	}{
		{"SUCCESSFUL", "✓"},
		{"FAILED", "✗"},
		{"ERROR", "✗"},
		{"STOPPED", "■"},
		{"", "·"},
		{"RUNNING", "·"},
	}

	for _, tt := range tests {
		t.Run(tt.result, func(t *testing.T) {
			got := stepIcon(tt.result, false)
			if got != tt.want {
				t.Errorf("stepIcon(%q, false) = %q, want %q", tt.result, got, tt.want)
			}
		})
	}
}

func TestResolveStep(t *testing.T) {
	steps := []bbcloud.PipelineStep{
		{UUID: "{step-1-uuid}", Name: "Build"},
		{UUID: "{step-2-uuid}", Name: "Test"},
		{UUID: "{step-3-uuid}", Name: "Deploy"},
	}

	tests := []struct {
		name       string
		identifier string
		wantName   string
		wantErr    bool
	}{
		{"by index 1", "1", "Build", false},
		{"by index 3", "3", "Deploy", false},
		{"index out of range", "4", "", true},
		{"index zero", "0", "", true},
		{"by name exact", "Build", "Build", false},
		{"by name case insensitive", "deploy", "Deploy", false},
		{"by UUID with braces", "{step-2-uuid}", "Test", false},
		{"by UUID without braces", "step-2-uuid", "Test", false},
		{"not found", "nonexistent", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveStep(steps, tt.identifier)
			if tt.wantErr {
				if err == nil {
					t.Errorf("resolveStep(%q) expected error, got nil", tt.identifier)
				}
				return
			}
			if err != nil {
				t.Errorf("resolveStep(%q) unexpected error: %v", tt.identifier, err)
				return
			}
			if got.Name != tt.wantName {
				t.Errorf("resolveStep(%q).Name = %q, want %q", tt.identifier, got.Name, tt.wantName)
			}
		})
	}
}

func TestFilterPipelines(t *testing.T) {
	user := &bbcloud.User{Username: "alice", Display: "Alice Smith"}
	pipelines := []bbcloud.Pipeline{
		makePipeline(1, "COMPLETED", "SUCCESSFUL", "main", user),
		makePipeline(2, "COMPLETED", "FAILED", "feature", user),
		makePipeline(3, "COMPLETED", "SUCCESSFUL", "main", nil),
		makePipeline(4, "PENDING", "", "develop", user),
	}

	tests := []struct {
		name string
		opts *listOptions
		want []int // expected build numbers
	}{
		{
			"filter by state",
			&listOptions{State: "COMPLETED"},
			[]int{1, 2, 3},
		},
		{
			"filter by result",
			&listOptions{Result: "FAILED"},
			[]int{2},
		},
		{
			"filter by ref",
			&listOptions{Ref: "main"},
			[]int{1, 3},
		},
		{
			"filter by creator username",
			&listOptions{Creator: "alice"},
			[]int{1, 2, 4},
		},
		{
			"combined filters",
			&listOptions{State: "COMPLETED", Ref: "main"},
			[]int{1, 3},
		},
		{
			"no match",
			&listOptions{Result: "ERROR"},
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterPipelines(pipelines, tt.opts)
			if len(got) != len(tt.want) {
				t.Fatalf("filterPipelines() returned %d pipelines, want %d", len(got), len(tt.want))
			}
			for i, p := range got {
				if p.BuildNumber != tt.want[i] {
					t.Errorf("pipeline[%d].BuildNumber = %d, want %d", i, p.BuildNumber, tt.want[i])
				}
			}
		})
	}
}

func TestIsTerminalState(t *testing.T) {
	tests := []struct {
		state string
		want  bool
	}{
		{"COMPLETED", true},
		{"HALTED", true},
		{"ERROR", true},
		{"PENDING", false},
		{"RUNNING", false},
		{"BUILDING", false},
	}

	for _, tt := range tests {
		t.Run(tt.state, func(t *testing.T) {
			if got := isTerminalState(tt.state); got != tt.want {
				t.Errorf("isTerminalState(%q) = %v, want %v", tt.state, got, tt.want)
			}
		})
	}
}

func TestNextInterval(t *testing.T) {
	current := 5 * time.Second
	max := 30 * time.Second

	next := nextInterval(current, max)

	// Should be approximately 7.5s (5 * 1.5) ± 15% jitter.
	lower := time.Duration(float64(7500*time.Millisecond) * 0.85)
	upper := time.Duration(float64(7500*time.Millisecond) * 1.15)

	if next < lower || next > upper {
		t.Errorf("nextInterval(5s, 30s) = %v, want between %v and %v", next, lower, upper)
	}

	// Test max capping.
	next = nextInterval(25*time.Second, max)
	upperMax := time.Duration(float64(max) * 1.15)
	if next > upperMax {
		t.Errorf("nextInterval(25s, 30s) = %v, should not exceed max (%v) + jitter", next, max)
	}
}

func TestFindFailedCommand(t *testing.T) {
	exitCode := func(n int) *int { return &n }

	tests := []struct {
		name string
		step bbcloud.PipelineStep
		want string
	}{
		{
			"no commands",
			bbcloud.PipelineStep{},
			"",
		},
		{
			"all successful",
			bbcloud.PipelineStep{
				ScriptCommands: []bbcloud.PipelineCommand{
					{Name: "npm install", ExitCode: exitCode(0)},
					{Name: "npm test", ExitCode: exitCode(0)},
				},
			},
			"",
		},
		{
			"failed script command",
			bbcloud.PipelineStep{
				ScriptCommands: []bbcloud.PipelineCommand{
					{Name: "npm install", ExitCode: exitCode(0)},
					{Name: "npm test", ExitCode: exitCode(1)},
				},
			},
			`exit 1 in "npm test"`,
		},
		{
			"failed setup command",
			bbcloud.PipelineStep{
				SetupCommands: []bbcloud.PipelineCommand{
					{Name: "docker pull", ExitCode: exitCode(127)},
				},
			},
			`exit 127 in "docker pull" (setup)`,
		},
		{
			"uses command field when name empty",
			bbcloud.PipelineStep{
				ScriptCommands: []bbcloud.PipelineCommand{
					{Command: "make build", ExitCode: exitCode(2)},
				},
			},
			`exit 2 in "make build"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findFailedCommand(tt.step)
			if got != tt.want {
				t.Errorf("findFailedCommand() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPipelineWebURL(t *testing.T) {
	tests := []struct {
		name      string
		pipeline  *bbcloud.Pipeline
		workspace string
		repo      string
		want      string
	}{
		{
			"from links",
			func() *bbcloud.Pipeline {
				p := &bbcloud.Pipeline{BuildNumber: 42}
				p.Links.HTML.Href = "https://bitbucket.org/ws/repo/addon/pipelines/home#!/results/42"
				return p
			}(),
			"ws", "repo",
			"https://bitbucket.org/ws/repo/addon/pipelines/home#!/results/42",
		},
		{
			"generated from workspace and repo",
			&bbcloud.Pipeline{BuildNumber: 10},
			"myws", "myrepo",
			"https://bitbucket.org/myws/myrepo/addon/pipelines/home#!/results/10",
		},
		{
			"no workspace",
			&bbcloud.Pipeline{BuildNumber: 10},
			"", "",
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pipelineWebURL(tt.pipeline, tt.workspace, tt.repo)
			if got != tt.want {
				t.Errorf("pipelineWebURL() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCreatorName(t *testing.T) {
	tests := []struct {
		name     string
		pipeline *bbcloud.Pipeline
		want     string
	}{
		{"nil creator", &bbcloud.Pipeline{}, ""},
		{"display name", &bbcloud.Pipeline{Creator: &bbcloud.User{Display: "Alice", Username: "alice"}}, "Alice"},
		{"username fallback", &bbcloud.Pipeline{Creator: &bbcloud.User{Username: "bob"}}, "bob"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := creatorName(tt.pipeline)
			if got != tt.want {
				t.Errorf("creatorName() = %q, want %q", got, tt.want)
			}
		})
	}
}

// Helper to construct test pipelines.
func makePipeline(buildNum int, state, result, ref string, creator *bbcloud.User) bbcloud.Pipeline {
	p := bbcloud.Pipeline{
		BuildNumber: buildNum,
		Creator:     creator,
	}
	p.State.Name = state
	p.State.Result.Name = result
	p.Target.Ref.Name = ref
	return p
}

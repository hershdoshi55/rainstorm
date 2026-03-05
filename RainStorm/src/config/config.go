package config

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"maps"
	"net"
	"os"
	"rainstorm-c7/rainstorm/core"
	"strconv"
	"strings"
	"time"
)

type Mode string

const (
	ModePingAck Mode = "pingack"
	ModeGossip  Mode = "gossip"
)

// Full, node-local config (bootstrap + cluster knobs).
type Config struct {
	// ------------- Rainstorm configuration ---------------
	RainstormHTTP        string               `json:"rainstorm_http"`          // ":15000" or "ip:port"
	RainstormConfig      core.RainstormConfig `json:"rainstorm_config"`        // rainstorm job config
	RainstormBinaryDir   string               `json:"rainstorm_binary_dir"`    // directory for rainstorm operator binaries
	RainstormLocalLogDir string               `json:"rainstorm_local_log_dir"` // directory for rainstorm local logs on worker VMs
	RainstormDatasetDir  string               `json:"rainstorm_dataset_dir"`   // directory for rainstorm datasets

	// ------------- HyDFS configuration -----------------

	HydfsHTTP         string `json:"hydfs_http"`     // ":10010" or "ip:port"
	HydfsFileDir      string `json:"hydfs_file_dir"` // directory for storing files on hydfs nodes
	LocalFileDir      string `json:"local_file_dir"` // directory for storing local files on nodes
	DatasetDir        string `json:"dataset_dir"`    // directory for datasets
	ReplicationFactor int    `json:"replication"`    // replication factor for stored files
	IsCtlClient       bool   `json:"is_ctl_client"`  // true if this node is a hydfsctl client

	// ------------- Membership configuration -------------

	// Cluster-wide (propagated)
	Version      int           `json:"version"`
	Mode         Mode          `json:"mode"`
	PingEvery    time.Duration `json:"ping_every"`
	PingFanout   int           `json:"ping_fanout"`
	GossipPeriod time.Duration `json:"gossip_period"`
	GossipFanout int           `json:"gossip_fanout"`
	TSuspect     time.Duration `json:"t_suspect"`
	TFail        time.Duration `json:"t_fail"`
	TCleanup     time.Duration `json:"t_cleanup"`
	DropRateRecv float64       `json:"drop_rate_recv"`

	// Local bootstrap (not propagated)
	SelfAddr           string `json:"self_addr"`                      // UDP "ip:port"
	BindAddr           string `json:"bind_addr"`                      // UDP bind "ip:port"
	IntroducerBindAddr string `json:"introducer_bind_addr,omitempty"` // introducer bind addr (optional, for NAT)
	Introducer         string `json:"introducer"`                     // UDP introducer (optional)
	IsIntroducer       bool   `json:"is_introducer"`                  // true if this node is the first
	MembershipHTTP     string `json:"membership_http"`                // ":8080" or "ip:port"

	// --------------- Environment config ----------------
	Env string `json:"env,omitempty"` // environment ("dev", "prod")
}

// Network-facing DTO. Only the cluster knobs go over the wire.
// Pointers let us detect "field not present" vs "present with zero value".
type ConfigDTO struct {
	Version      int     `json:"version,omitempty"`
	Mode         string  `json:"mode,omitempty"`
	PingEvery    string  `json:"ping_every,omitempty"`
	PingFanout   int     `json:"ping_fanout,omitempty"`
	GossipPeriod string  `json:"gossip_period,omitempty"`
	GossipFanout int     `json:"gossip_fanout,omitempty"`
	TSuspect     string  `json:"t_suspect,omitempty"`
	TFail        string  `json:"t_fail,omitempty"`
	TCleanup     string  `json:"t_cleanup,omitempty"`
	DropRateRecv float64 `json:"drop_rate_recv,omitempty"`
}

func Defaults() Config {

	return Config{
		// ------------- Rainstorm configuration ---------------
		RainstormHTTP: ":15000",
		RainstormConfig: core.RainstormConfig{
			NStages:            3,
			NTasksPerStage:     5,
			RainstormSourceDir: "/rainstorm_file_store",
			HydfsDestFileName:  "rainstorm_output.txt",
			JobConfig: core.JobConfig{
				Stages:       make(map[int]core.StageConfig),
				ExactlyOnce:  true,
				Autoscale:    false,
				InputRate:    100,
				LowWatermark: 50,
			},
		},
		RainstormBinaryDir:   "/binaries",
		RainstormLocalLogDir: "/rainstorm_local_logs",
		RainstormDatasetDir:  "/rainstorm-c7/rainstorm_dataset",

		// ------------- HyDFS configuration -----------------
		HydfsHTTP:         ":10010",
		HydfsFileDir:      "/hydfs_file_store",
		LocalFileDir:      "/local_file_store",
		DatasetDir:        "/rainstorm-c7/dataset",
		ReplicationFactor: 3,
		IsCtlClient:       false,

		// ------------- Membership configuration -------------
		// propagated
		Version: 1,
		//Mode:    ModePingAck,
		Mode:      ModeGossip,
		PingEvery: 200 * time.Millisecond,
		//AckTimeout:   250 * time.Millisecond,
		PingFanout:   1,
		GossipPeriod: 300 * time.Millisecond,
		GossipFanout: 3,
		TSuspect:     00000 * time.Millisecond,
		TFail:        2000 * time.Millisecond,
		TCleanup:     3000 * time.Millisecond,
		DropRateRecv: 0.0,

		// local bootstrap

		//get self address from the server hostname
		SelfAddr:           "127.0.0.1:5000", // TODO: derive this from OS hostname
		BindAddr:           ":5000",
		IntroducerBindAddr: ":6000",
		Introducer:         "",
		IsIntroducer:       false,
		MembershipHTTP:     ":8080",

		// --------------- Environment config ----------------
		Env: "prod",
	}
}

/* ------------------------- Centralized validation ------------------------- */

// ValidateLocalBootstrap: only local/identity bits.
func ValidateLocalBootstrap(c *Config) error {
	log.Printf("Validating local bootstrap: self=%q bind=%q introducer=%q is-introducer=%v admin=%q\n",
		c.SelfAddr, c.BindAddr, c.Introducer, c.IsIntroducer, c.MembershipHTTP)
	// basic addr checks

	if err := validateUDPAddr(c.SelfAddr); err != nil {
		return fmt.Errorf("self addr: %w", err)
	}
	if err := validateUDPAddr(c.BindAddr); err != nil {
		return fmt.Errorf("bind addr: %w", err)
	}
	// exactly one of Introducer or IsIntroducer
	if (c.Introducer == "" && !c.IsIntroducer) || (c.Introducer != "" && c.IsIntroducer) {
		return errors.New("must specify either introducer OR is-introducer")
	}
	if c.Introducer != "" {
		if err := validateUDPAddr(c.Introducer); err != nil {
			return fmt.Errorf("introducer: %w", err)
		}
	}
	if err := validateAdminAddr(c.MembershipHTTP); err != nil {
		return fmt.Errorf("membership http: %w", err)
	}
	return nil
}

// ValidateClusterParams normalizes/validates cluster-wide knobs (one place).
func ValidateClusterParams(c *Config) error {
	// Mode
	if c.Mode != ModePingAck && c.Mode != ModeGossip {
		return errors.New("invalid mode")
	}
	// Periods & counts
	if c.PingEvery <= 0 {
		return errors.New("ping_every must be > 0")
	}
	if c.PingFanout <= 0 {
		return errors.New("ping_fanout must be > 0")
	}
	if c.GossipPeriod <= 0 {
		return errors.New("gossip_period must be > 0")
	}
	if c.GossipFanout <= 0 {
		return errors.New("gossip_fanout must be > 0")
	}
	// Timers monotonic
	if c.TSuspect < 0 {
		return errors.New("t_suspect must be >= 0")
	}
	if c.TFail <= 0 {
		return errors.New("t_fail must be > 0")
	}
	if c.TCleanup <= 0 {
		return errors.New("t_cleanup must be > 0")
	}
	// Drop rate clamp
	if c.DropRateRecv < 0 || c.DropRateRecv > 1 {
		return errors.New("drop_rate_recv must be in [0,1]")
	}
	return nil
}

/* ------------------------ Flags (call once at boot) ----------------------- */

func LoadFromFlags() (Config, error) {
	def := Defaults()

	// rainstorm flags
	flag.StringVar(&def.RainstormHTTP, "rainstorm-http", def.RainstormHTTP, "Rainstorm HTTP addr (:port or ip:port)")
	flag.IntVar(&def.RainstormConfig.NStages, "n-stages", def.RainstormConfig.NStages, "number of stages in the dataflow")
	flag.IntVar(&def.RainstormConfig.NTasksPerStage, "n-tasks-per-stage", def.RainstormConfig.NTasksPerStage, "number of tasks per stage")
	// parse stage config map from string
	flag.Func("stage-config-map",
		"stage configs, e.g. \"stage0 source 100 200 stage1 map x2 stage2 sink\"",
		func(s string) error {
			m, err := ParseStageConfigMap(s)
			if err != nil {
				return err
			}
			// merge/overwrite into def.StageConfigMap
			maps.Copy(def.RainstormConfig.JobConfig.Stages, m)
			return nil
		})

	flag.StringVar(&def.RainstormConfig.RainstormSourceDir, "rainstorm-source-dir", def.RainstormConfig.RainstormSourceDir, "directory for rainstorm source files")
	flag.StringVar(&def.RainstormBinaryDir, "rainstorm-binary-dir", def.RainstormBinaryDir, "directory for rainstorm operator binaries")
	flag.StringVar(&def.RainstormLocalLogDir, "rainstorm-local-log-dir", def.RainstormLocalLogDir, "directory for rainstorm local logs on worker VMs")
	flag.StringVar(&def.RainstormConfig.HydfsDestFileName, "hydfs-dest-file-name", def.RainstormConfig.HydfsDestFileName, "destination filename in hydfs for rainstorm output")
	flag.BoolVar(&def.RainstormConfig.JobConfig.ExactlyOnce, "exactly-once", def.RainstormConfig.JobConfig.ExactlyOnce, "whether to use exactly-once processing")
	flag.BoolVar(&def.RainstormConfig.JobConfig.Autoscale, "autoscale", def.RainstormConfig.JobConfig.Autoscale, "whether to enable autoscaling")
	flag.IntVar(&def.RainstormConfig.JobConfig.InputRate, "input-rate", def.RainstormConfig.JobConfig.InputRate, "input rate (tuples/sec) for source stage")
	flag.Float64Var(&def.RainstormConfig.JobConfig.LowWatermark, "low-watermark", def.RainstormConfig.JobConfig.LowWatermark, "low watermark for autoscaling")
	flag.Float64Var(&def.RainstormConfig.JobConfig.HighWatermark, "high-watermark", def.RainstormConfig.JobConfig.HighWatermark, "high watermark for autoscaling")

	// hydfs flags
	flag.StringVar(&def.HydfsHTTP, "hydfs-http", def.HydfsHTTP, "HyDFS HTTP addr (:port or ip:port)")
	flag.StringVar(&def.HydfsFileDir, "hydfs-file-dir", def.HydfsFileDir, "directory for storing files on hydfs nodes")
	flag.StringVar(&def.LocalFileDir, "local-file-dir", def.LocalFileDir, "directory for storing local files on nodes")
	flag.IntVar(&def.ReplicationFactor, "replication-factor", def.ReplicationFactor, "replication factor for stored files")
	flag.BoolVar(&def.IsCtlClient, "is-ctl-client", def.IsCtlClient, "true if this config is being loaded into a hydfsctl client")

	// bootstrap flags
	flag.StringVar(&def.BindAddr, "bind", def.BindAddr, "bind UDP addr (ip:port)")
	flag.StringVar(&def.SelfAddr, "self", def.SelfAddr, "self advertised UDP addr (ip:port)")
	flag.StringVar(&def.Introducer, "introducer", def.Introducer, "introducer UDP addr (optional)")
	flag.BoolVar(&def.IsIntroducer, "is-introducer", def.IsIntroducer, "this node is the introducer")
	flag.StringVar(&def.MembershipHTTP, "membership-http", def.MembershipHTTP, "membership http addr (:port or ip:port)")

	// cluster knobs (introducer or CLI changes)
	mode := flag.String("mode", string(def.Mode), "protocol mode: pingack|gossip")
	flag.DurationVar(&def.PingEvery, "ping-every", def.PingEvery, "ping period")
	flag.IntVar(&def.PingFanout, "ping-fanout", def.PingFanout, "ping fanout")
	flag.DurationVar(&def.GossipPeriod, "gossip-period", def.GossipPeriod, "gossip period")
	flag.IntVar(&def.GossipFanout, "gossip-fanout", def.GossipFanout, "gossip fanout")
	flag.DurationVar(&def.TSuspect, "t-suspect", def.TSuspect, "suspect after no alive for this")
	flag.DurationVar(&def.TFail, "t-fail", def.TFail, "fail after suspect/no updates")
	flag.DurationVar(&def.TCleanup, "t-cleanup", def.TCleanup, "delete after failed/left")
	flag.Float64Var(&def.DropRateRecv, "drop-recv", def.DropRateRecv, "receiver drop rate [0..1]")

	flag.StringVar(&def.Env, "env", def.Env, "environment (dev|prod)")

	flag.Parse()

	if def.Env == "dev" {
		def.SelfAddr = "127.0.0.1" + def.BindAddr
		def.RainstormConfig.RainstormSourceDir = "../" + def.BindAddr[1:] + def.RainstormConfig.RainstormSourceDir
		def.RainstormBinaryDir = "../" + def.BindAddr[1:] + def.RainstormBinaryDir
		def.HydfsFileDir = "../" + def.BindAddr[1:] + def.HydfsFileDir
		def.LocalFileDir = "../" + def.BindAddr[1:] + def.LocalFileDir
	} else {
		hostname, _ := os.Hostname()
		def.SelfAddr = hostname + def.BindAddr
		def.RainstormConfig.RainstormSourceDir = "/home/mp4" + def.RainstormConfig.RainstormSourceDir
		def.RainstormDatasetDir = "/home/mp4" + def.RainstormDatasetDir
		def.RainstormLocalLogDir = "/home/mp4" + def.RainstormLocalLogDir
		def.RainstormBinaryDir = "/home/mp4" + def.RainstormBinaryDir
		def.HydfsFileDir = "/home/mp4" + def.HydfsFileDir
		def.LocalFileDir = "/home/mp4" + def.LocalFileDir
		def.DatasetDir = "/home/mp4" + def.DatasetDir
	}

	// apply selected mode
	switch Mode(*mode) {
	case ModePingAck, ModeGossip:
		def.Mode = Mode(*mode)
	default:
		return Config{}, errors.New("invalid mode")
	}

	if def.IsCtlClient {
		// skip membership validations for hydfsctl clients
		return def, nil
	}
	// centralized checks
	if err := ValidateLocalBootstrap(&def); err != nil {
		return Config{}, err
	}
	if err := ValidateClusterParams(&def); err != nil {
		return Config{}, err
	}
	return def, nil
}

/* -------------------- Config <-> DTO conversions/merge ------------------- */

// ToDTO builds a DTO with ALL cluster knobs (good default for gossip/forward).
func (c Config) ToDTO() ConfigDTO {
	return ConfigDTO{
		Version:      c.Version,
		Mode:         string(c.Mode),
		PingEvery:    c.PingEvery.String(),
		PingFanout:   c.PingFanout,
		GossipPeriod: c.GossipPeriod.String(),
		GossipFanout: c.GossipFanout,
		TSuspect:     c.TSuspect.String(),
		TFail:        c.TFail.String(),
		TCleanup:     c.TCleanup.String(),
		DropRateRecv: c.DropRateRecv,
	}
}

// ApplyDTO merges the provided DTO into c (in-place) with version checks.
// DTO is expected to be COMPLETE. Returns (changed, error).
func (c *Config) ApplyDTO(dto ConfigDTO) (bool, error) {
	next := *c

	// Version handling (must be strictly newer to apply)
	if dto.Version <= c.Version {
		return false, nil
	}
	next.Version = dto.Version

	// Mode (required)
	if dto.Mode == "" {
		return false, errors.New("mode missing")
	}
	m := Mode(dto.Mode)
	if m != ModePingAck && m != ModeGossip {
		return false, errors.New("invalid mode")
	}
	next.Mode = m

	// Durations (required; TSuspect may be "0s")
	var err error
	if dto.PingEvery == "" {
		return false, errors.New("ping_every missing")
	}
	if next.PingEvery, err = time.ParseDuration(dto.PingEvery); err != nil || next.PingEvery <= 0 {
		return false, errors.New("invalid ping_every")
	}

	if dto.GossipPeriod == "" {
		return false, errors.New("gossip_period missing")
	}
	if next.GossipPeriod, err = time.ParseDuration(dto.GossipPeriod); err != nil || next.GossipPeriod <= 0 {
		return false, errors.New("invalid gossip_period")
	}

	if dto.TSuspect == "" {
		return false, errors.New("t_suspect missing")
	}
	if next.TSuspect, err = time.ParseDuration(dto.TSuspect); err != nil || next.TSuspect < 0 {
		return false, errors.New("invalid t_suspect")
	}

	if dto.TFail == "" {
		return false, errors.New("t_fail missing")
	}
	if next.TFail, err = time.ParseDuration(dto.TFail); err != nil || next.TFail <= 0 {
		return false, errors.New("invalid t_fail")
	}

	if dto.TCleanup == "" {
		return false, errors.New("t_cleanup missing")
	}
	if next.TCleanup, err = time.ParseDuration(dto.TCleanup); err != nil || next.TCleanup <= 0 {
		return false, errors.New("invalid t_cleanup")
	}

	// Int knobs (required)
	if dto.PingFanout <= 0 {
		return false, errors.New("ping_fanout must be > 0")
	}
	next.PingFanout = dto.PingFanout

	if dto.GossipFanout <= 0 {
		return false, errors.New("gossip_fanout must be > 0")
	}
	next.GossipFanout = dto.GossipFanout

	// Float knob (required)
	if dto.DropRateRecv < 0 || dto.DropRateRecv > 1 {
		return false, errors.New("drop_rate_recv must be in [0,1]")
	}
	next.DropRateRecv = dto.DropRateRecv

	// Centralized normalization/validation for cluster knobs
	if err := ValidateClusterParams(&next); err != nil {
		return false, err
	}

	// Commit
	*c = next
	return true, nil
}

/* ------------------------------ helpers ---------------------------------- */

func validateUDPAddr(addr string) error {
	if addr == "" {
		return errors.New("empty")
	}
	if _, err := net.ResolveUDPAddr("udp", addr); err != nil {
		return err
	}
	return nil
}

// Accepts ":port" or "ip:port".
func validateAdminAddr(addr string) error {
	if addr == "" {
		return errors.New("empty")
	}
	if strings.HasPrefix(addr, ":") {
		p := strings.TrimPrefix(addr, ":")
		if _, err := strconv.Atoi(p); err != nil || p == "" {
			return errors.New("bad port")
		}
		return nil
	}
	_, err := net.ResolveTCPAddr("tcp", addr)
	return err
}

// ApplyRemote applies a DTO that must come from the network.
// Since DTO is value-based, Version is always present; ApplyDTO enforces Version > current.
func (c *Config) ApplyRemote(dto ConfigDTO) (bool, error) {
	return c.ApplyDTO(dto)
}

// BumpAndApplyLocal applies a DTO from local CLI/admin.
// Ensures the version is bumped locally (current+1) if not strictly higher.
func (c *Config) BumpAndApplyLocal(dto ConfigDTO) (bool, error) {
	if dto.Version <= c.Version {
		dto.Version = c.Version + 1
	}
	return c.ApplyDTO(dto)
}

// parseStageConfigMap parses a stage config map from a string input.
// The input format is expected to be:
// "stage0 operator arg1 arg2 ... stage1 operator arg1 arg2 ..."
// For example:
// "stage0 filter 100 200 stage1 map x2 stage2 aggregate"
func ParseStageConfigMap(input string) (map[int]core.StageConfig, error) {
	result := make(map[int]core.StageConfig)

	input = strings.TrimSpace(input)
	if input == "" {
		return result, nil // empty map is fine
	}

	tokens := strings.Fields(input)
	i := 0
	for i < len(tokens) {
		tok := tokens[i]

		if !strings.HasPrefix(tok, "stage") {
			return nil, fmt.Errorf("expected token starting with 'stage', got %q", tok)
		}

		stageStr := strings.TrimPrefix(tok, "stage")
		stageIdx, err := strconv.Atoi(stageStr)
		if err != nil {
			return nil, fmt.Errorf("invalid stage index in token %q: %w", tok, err)
		}

		// We must have at least one more token for the operator.
		if i+1 >= len(tokens) {
			return nil, fmt.Errorf("missing operator after %q", tok)
		}

		operator := tokens[i+1]
		i += 2

		args := []string{}
		for i < len(tokens) && !strings.HasPrefix(tokens[i], "stage") {
			args = append(args, tokens[i])
			i++
		}

		if _, exists := result[stageIdx]; exists {
			return nil, fmt.Errorf("duplicate config for stage %d", stageIdx)
		}

		result[stageIdx] = core.StageConfig{
			Operator: operator,
			Args:     args,
		}
	}

	return result, nil
}

// Output:
// cfg.StageConfigMap[0] = StageConfig{
//     Operator: "./filter",
//     Args:     []string{"10", "20"},
//     NumTasks: 3,
// }

// cfg.StageConfigMap[1] = StageConfig{
//     Operator: "./map",
//     Args:     []string{"foo", "bar"},
//     NumTasks: 3,
// }

// cfg.StageConfigMap[2] = StageConfig{
//     Operator: "./aggregate",
//     Args:     []string{},
//     NumTasks: 3,
// }

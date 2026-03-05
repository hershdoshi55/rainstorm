package rainstorm_daemon

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"rainstorm-c7/config"
	nodeid "rainstorm-c7/membership/node"
	"rainstorm-c7/membership/store"
	"rainstorm-c7/rainstorm/cmd/client"
	"rainstorm-c7/rainstorm/cmd/server"
	"rainstorm-c7/rainstorm/core"
	"rainstorm-c7/rainstorm/leader"
	agent "rainstorm-c7/rainstorm/worker"
	generic_utils "rainstorm-c7/utils"
	"time"
)

type RainstormDaemon struct {
	cfg        config.Config
	httpClient *client.HTTPClient
	httpServer *server.HTTPServer
	cancel     context.CancelFunc
}

func Run(cfg config.Config, st *store.Store, selfNodeID nodeid.NodeID) (*RainstormDaemon, error) {

	// Context for background services (ring manager, shutdown)
	ctx, cancel := context.WithCancel(context.Background())
	httpClient := client.NewHTTPClient(cfg)

	var newleader *leader.Leader
	var newWorkerAgent *agent.Agent
	var handlers server.Handlers

	if err := cleanupAndEnsureLogDirExists(cfg); err != nil {
		log.Printf("error cleaning up log dir: %v\n", err)
		return nil, err
	}

	if err := cleanupAndEnsureSrcDirExists(cfg); err != nil {
		log.Printf("error cleaning up source dir: %v\n", err)
		return nil, err
	}
	if err := cleanupAndEnsureSrcDirExists(cfg); err != nil {
		log.Printf("error cleaning up source dir: %v\n", err)
		return nil, err
	}

	if err := copyCSVFiles(cfg.RainstormDatasetDir, cfg.RainstormConfig.RainstormSourceDir); err != nil {
		log.Printf("error copying CSV files to local log dir: %v\n", err)
		return nil, err
	}

	if cfg.IsIntroducer {
		log.Println("Starting leader services...")
		newleader = leader.NewLeader(ctx, st, httpClient, cfg)

		handlers = server.Handlers{
			OnTaskFailed:              newleader.HandleTaskFailed,
			OnStartRainstorm:          newleader.StartJob,
			OnKillTaskFromUser:        newleader.KillTaskRequestFromUser,
			OnGetTaskMap:              newleader.GetTaskMap,
			OnTupleAck:                newleader.HandleTupleAck,
			OnFinalStageTaskCompleted: newleader.HandleFinalStageTaskCompleted,
			OnTaskMetrics:             newleader.ReportTaskInputRate,
		}

	} else {
		log.Println("Starting worker services...")
		newWorkerAgent = agent.NewAgent(cfg.Introducer, core.WorkerInfo{
			NodeID: selfNodeID,
		}, httpClient, cfg)

		handlers = server.Handlers{
			OnStart:         newWorkerAgent.StartTask,
			OnStop:          newWorkerAgent.StopTask,
			OnKill:          newWorkerAgent.KillTask,
			OnTaskMapUpdate: newWorkerAgent.HandleTaskMapUpdate,
			OnReceiveTuple:  newWorkerAgent.ProcessIncomingTuple,
			OnTupleAck:      newWorkerAgent.HandleTupleAck,
		}
	}

	httpServer := server.NewServer(cfg.RainstormHTTP, handlers)

	rainstormd := &RainstormDaemon{
		cfg:        cfg,
		httpClient: httpClient,
		httpServer: httpServer,
		cancel:     cancel,
	}

	go func() {
		if err := httpServer.Start(); err != nil {
			log.Printf("rainstorm HTTP server: %v\n", err)
		}
	}()

	return rainstormd, nil
}

// Close gracefully stops HTTP, ring manager subscription, and background context.
func (d *RainstormDaemon) Close() {
	// Stop accepting new HTTP conns and give in-flight up to 5s to finish
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if d.httpServer != nil {
		_ = d.httpServer.Shutdown(ctx)
	}

	// Cancel background context (if anything else is using it)
	if d.cancel != nil {
		d.cancel()
	}

	// Optional: flush logs, sync storage state, etc.
	log.Println("rainstorm daemon closed")
}

func copyCSVFiles(srcDir, dstDir string) error {
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		// If datasetDir doesn't exist, just no-op.
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if filepath.Ext(name) != ".csv" {
			continue
		}
		src := filepath.Join(srcDir, name)
		dst := filepath.Join(dstDir, name)
		if err := copyFile(src, dst); err != nil {
			return err
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst) // 0644
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	return err
}

func cleanupAndEnsureLogDirExists(config config.Config) error {

	// If Dir exists, remove all contents, else create it
	if _, err := os.Stat(config.RainstormLocalLogDir); os.IsNotExist(err) {
		err := os.MkdirAll(config.RainstormLocalLogDir, os.ModePerm)
		if err != nil {
			log.Fatalf(generic_utils.Red+"[daemon] failed to create RainstormLocalLogDir %s: %v\n"+generic_utils.Reset, config.RainstormLocalLogDir, err)
			return err
		}
	} else {
		err := os.RemoveAll(config.RainstormLocalLogDir)
		if err != nil {
			log.Printf("error in remove dir %s: %v", config.RainstormLocalLogDir, err)
			return err
		}
		err = os.MkdirAll(config.RainstormLocalLogDir, os.ModePerm)
		if err != nil {
			log.Fatalf(generic_utils.Red+"[daemon] failed to create RainstormLocalLogDir %s: %v\n"+generic_utils.Reset, config.RainstormLocalLogDir, err)
			return err
		}
	}
	return nil
}

func cleanupAndEnsureSrcDirExists(config config.Config) error {
	// If Dir exists, remove all contents, else create it
	if _, err := os.Stat(config.RainstormConfig.RainstormSourceDir); os.IsNotExist(err) {
		err := os.MkdirAll(config.RainstormConfig.RainstormSourceDir, os.ModePerm)
		if err != nil {
			log.Fatalf(generic_utils.Red+"[daemon] failed to create RainstormSourceDir %s: %v\n"+generic_utils.Reset, config.RainstormConfig.RainstormSourceDir, err)
			return err
		}
	} else {
		err := os.RemoveAll(config.RainstormConfig.RainstormSourceDir)
		if err != nil {
			log.Printf("error in remove dir %s: %v", config.RainstormConfig.RainstormSourceDir, err)
			return err
		}
		err = os.MkdirAll(config.RainstormConfig.RainstormSourceDir, os.ModePerm)
		if err != nil {
			log.Fatalf(generic_utils.Red+"[daemon] failed to create RainstormSourceDir %s: %v\n"+generic_utils.Reset, config.RainstormConfig.RainstormSourceDir, err)
			return err
		}
	}
	return nil
}

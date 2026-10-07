package virtual_test

import (
	"context"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-clientd/internal/mock"
	cd_vfs "github.com/buildbarn/bb-clientd/pkg/filesystem/virtual"
	re_vfs "github.com/buildbarn/bb-remote-execution/pkg/filesystem/virtual"
	"github.com/buildbarn/bb-remote-execution/pkg/proto/bazeloutputservice"
	bazeloutputservicerev2 "github.com/buildbarn/bb-remote-execution/pkg/proto/bazeloutputservice/rev2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

type busyOutputPathFactory struct {
	start func(path.Component) cd_vfs.OutputPath
	clean func(path.Component) error
}

func (f busyOutputPathFactory) StartInitialBuild(id path.Component, _ re_vfs.CASFileFactory, _ digest.Function, _ util.ErrorLogger) cd_vfs.OutputPath {
	return f.start(id)
}

func (f busyOutputPathFactory) Clean(id path.Component) error {
	return f.clean(id)
}

func newBusyDirectory(t *testing.T, factory cd_vfs.OutputPathFactory) (*cd_vfs.BazelOutputServiceDirectory, *mock.MockStatefulDirectoryHandle) {
	t.Helper()
	ctrl := gomock.NewController(t)
	allocator := mock.NewMockStatefulHandleAllocator(ctrl)
	allocation := mock.NewMockStatefulHandleAllocation(ctrl)
	handle := mock.NewMockStatefulDirectoryHandle(ctrl)
	allocator.EXPECT().New().Return(allocation).AnyTimes()
	allocation.EXPECT().AsStatefulDirectory(gomock.Any()).Return(handle)
	allocation.EXPECT().AsStatelessAllocator().Return(mock.NewMockStatelessHandleAllocator(ctrl)).AnyTimes()
	handle.EXPECT().GetAttributes(gomock.Any(), gomock.Any()).AnyTimes()
	return cd_vfs.NewBazelOutputServiceDirectory(allocator, factory, mock.NewMockBlobAccess(ctrl), mock.NewMockBlobAccess(ctrl), mock.NewMockDirectoryFetcher(ctrl), mock.NewMockSymlinkFactory(ctrl), 10000), handle
}

func startBusyBuild(ctx context.Context, d *cd_vfs.BazelOutputServiceDirectory, base, build string) error {
	args, err := anypb.New(&bazeloutputservicerev2.StartBuildArgs{DigestFunction: remoteexecution.DigestFunction_SHA256})
	if err != nil {
		return err
	}
	_, err = d.StartBuild(ctx, &bazeloutputservice.StartBuildRequest{OutputBaseId: base, BuildId: build, Args: args, OutputPathPrefix: "/outputs"})
	return err
}

func runBusyRPC(call func() error) <-chan error {
	done := make(chan error, 1)
	go func() { done <- call() }()
	return done
}

func requireBusyRPCPending(t *testing.T, done <-chan error) {
	t.Helper()
	synctest.Wait()
	select {
	case err := <-done:
		t.Fatalf("RPC completed while output path was busy: %v", err)
	default:
	}
}

func TestBazelOutputServiceDirectoryBusyFinalization(t *testing.T) {
	for _, next := range []string{"StartBuild", "FinalizeBuild", "Clean", "StageArtifacts", "BatchStat"} {
		t.Run(next, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				ctrl := gomock.NewController(t)
				a, b := mock.NewMockOutputPath(ctrl), mock.NewMockOutputPath(ctrl)
				a.EXPECT().FilterChildren(gomock.Any()).AnyTimes()
				b.EXPECT().FilterChildren(gomock.Any())
				entered, unblock := make(chan struct{}), make(chan struct{})
				release := sync.OnceFunc(func() { close(unblock) })
				defer release()
				a.EXPECT().FinalizeBuild(gomock.Any(), gomock.Any()).Do(func(context.Context, digest.Function) {
					close(entered)
					<-unblock
				})
				d, handle := newBusyDirectory(t, busyOutputPathFactory{start: func(id path.Component) cd_vfs.OutputPath {
					if id.String() == "a" {
						return a
					}
					return b
				}})
				require.NoError(t, startBusyBuild(ctx, d, "a", "old"))
				finalized := runBusyRPC(func() error {
					_, err := d.FinalizeBuild(ctx, &bazeloutputservice.FinalizeBuildRequest{BuildId: "old"})
					return err
				})
				<-entered

				// A canceled waiter must not end finalization or prevent later waiters.
				waitCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				canceled := runBusyRPC(func() error { return startBusyBuild(waitCtx, d, "a", "canceled") })
				requireBusyRPCPending(t, canceled)
				cancel()
				require.Equal(t, codes.Canceled, status.Code(<-canceled))

				if next == "Clean" {
					a.EXPECT().RemoveAllChildren(true)
					handle.EXPECT().NotifyRemoval(path.MustNewComponent("a"))
				}
				queued := runBusyRPC(func() error {
					switch next {
					case "StartBuild":
						return startBusyBuild(ctx, d, "a", "new")
					case "FinalizeBuild":
						_, err := d.FinalizeBuild(ctx, &bazeloutputservice.FinalizeBuildRequest{BuildId: "old"})
						return err
					case "Clean":
						_, err := d.Clean(ctx, &bazeloutputservice.CleanRequest{OutputBaseId: "a"})
						return err
					case "StageArtifacts":
						_, err := d.StageArtifacts(ctx, &bazeloutputservice.StageArtifactsRequest{BuildId: "old"})
						return err
					default:
						_, err := d.BatchStat(ctx, &bazeloutputservice.BatchStatRequest{BuildId: "old"})
						return err
					}
				})
				requireBusyRPCPending(t, queued)

				// Unrelated bases continue to make progress while a's finalizer waits.
				require.NoError(t, startBusyBuild(ctx, d, "b", "other"))
				_, err := d.StageArtifacts(ctx, &bazeloutputservice.StageArtifactsRequest{BuildId: "other"})
				require.NoError(t, err)
				_, err = d.BatchStat(ctx, &bazeloutputservice.BatchStatRequest{BuildId: "other"})
				require.NoError(t, err)
				release()
				require.NoError(t, <-finalized)
				err = <-queued
				if next == "StageArtifacts" || next == "BatchStat" {
					require.Equal(t, codes.FailedPrecondition, status.Code(err))
				} else {
					require.NoError(t, err)
				}
			})
		})
	}
}

func TestBazelOutputServiceDirectoryBusyStartAndClean(t *testing.T) {
	for _, operation := range []string{"Restore", "Filter", "FilterFailure", "UnrestoredClean", "UnrestoredCleanFailure", "CleanFailure", "CleanNotification"} {
		t.Run(operation, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				ctrl := gomock.NewController(t)
				a, b := mock.NewMockOutputPath(ctrl), mock.NewMockOutputPath(ctrl)
				entered, unblock := make(chan struct{}), make(chan struct{})
				release := sync.OnceFunc(func() { close(unblock) })
				defer release()
				hold := func() { close(entered); <-unblock }
				starts, filters := 0, 0
				a.EXPECT().FilterChildren(gomock.Any()).DoAndReturn(func(re_vfs.ChildFilter) error {
					filters++
					if filters == 1 && (operation == "Filter" || operation == "FilterFailure") {
						hold()
						if operation == "FilterFailure" {
							return status.Error(codes.Internal, "filter failed")
						}
					}
					return nil
				}).AnyTimes()
				b.EXPECT().FilterChildren(gomock.Any())
				d, handle := newBusyDirectory(t, busyOutputPathFactory{
					start: func(id path.Component) cd_vfs.OutputPath {
						if id.String() == "b" {
							return b
						}
						starts++
						if starts == 1 && operation == "Restore" {
							hold()
						}
						return a
					},
					clean: func(path.Component) error {
						hold()
						if operation == "UnrestoredCleanFailure" {
							return status.Error(codes.Internal, "clean failed")
						}
						return nil
					},
				})
				if operation == "CleanFailure" || operation == "CleanNotification" {
					require.NoError(t, startBusyBuild(ctx, d, "a", "old"))
					if operation == "CleanFailure" {
						a.EXPECT().RemoveAllChildren(true).DoAndReturn(func(bool) error {
							hold()
							return status.Error(codes.Internal, "clean failed")
						})
					} else {
						a.EXPECT().RemoveAllChildren(true)
						handle.EXPECT().NotifyRemoval(path.MustNewComponent("a")).Do(func(path.Component) { hold() })
					}
				}
				first := runBusyRPC(func() error {
					switch operation {
					case "Restore", "Filter", "FilterFailure":
						return startBusyBuild(ctx, d, "a", "old")
					default:
						_, err := d.Clean(ctx, &bazeloutputservice.CleanRequest{OutputBaseId: "a"})
						return err
					}
				})
				<-entered
				if operation == "Restore" || operation == "UnrestoredClean" || operation == "UnrestoredCleanFailure" || operation == "CleanNotification" {
					// Busy placeholders must not be exposed as partially initialized roots.
					var attributes re_vfs.Attributes
					_, s := d.VirtualLookup(ctx, path.MustNewComponent("a"), 0, &attributes)
					require.Equal(t, re_vfs.StatusErrNoEnt, s)
					d.VirtualGetAttributes(ctx, re_vfs.AttributesMaskLinkCount, &attributes)
					require.Equal(t, re_vfs.EmptyDirectoryLinkCount, attributes.GetLinkCount())
					reporter := mock.NewMockDirectoryEntryReporter(ctrl)
					require.Equal(t, re_vfs.StatusOK, d.VirtualReadDir(ctx, 0, 0, reporter))
				}
				second := runBusyRPC(func() error { return startBusyBuild(ctx, d, "a", "next") })
				requireBusyRPCPending(t, second)
				require.NoError(t, startBusyBuild(ctx, d, "b", "other"))
				release()
				err := <-first
				if operation == "FilterFailure" || operation == "UnrestoredCleanFailure" || operation == "CleanFailure" {
					require.Equal(t, codes.Internal, status.Code(err))
				} else {
					require.NoError(t, err)
				}
				require.NoError(t, <-second)
				if operation == "CleanNotification" {
					require.Equal(t, 2, starts, "start must look up the base again after cleanup")
				} else {
					require.Equal(t, 1, starts)
				}
				var attributes re_vfs.Attributes
				d.VirtualGetAttributes(ctx, re_vfs.AttributesMaskLinkCount, &attributes)
				require.Equal(t, re_vfs.EmptyDirectoryLinkCount+2, attributes.GetLinkCount())
			})
		})
	}
}

func TestBazelOutputServiceDirectoryRequestsWaitForBusyStart(t *testing.T) {
	for _, test := range []struct{ phase, next string }{
		{"Restore", "Clean"},
		{"Filter", "Clean"},
		{"Filter", "StageArtifacts"},
		{"Filter", "BatchStat"},
	} {
		t.Run(test.phase+"/"+test.next, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				ctrl := gomock.NewController(t)
				root := mock.NewMockOutputPath(ctrl)
				entered, unblock := make(chan struct{}), make(chan struct{})
				release := sync.OnceFunc(func() { close(unblock) })
				defer release()
				hold := func() { close(entered); <-unblock }
				root.EXPECT().FilterChildren(gomock.Any()).DoAndReturn(func(re_vfs.ChildFilter) error {
					if test.phase == "Filter" {
						hold()
					}
					return nil
				})
				d, handle := newBusyDirectory(t, busyOutputPathFactory{start: func(path.Component) cd_vfs.OutputPath {
					if test.phase == "Restore" {
						hold()
					}
					return root
				}})
				if test.next == "Clean" {
					root.EXPECT().RemoveAllChildren(true)
					handle.EXPECT().NotifyRemoval(path.MustNewComponent("a"))
				}
				started := runBusyRPC(func() error { return startBusyBuild(ctx, d, "a", "build") })
				<-entered
				queued := runBusyRPC(func() error {
					switch test.next {
					case "Clean":
						_, err := d.Clean(ctx, &bazeloutputservice.CleanRequest{OutputBaseId: "a"})
						return err
					case "StageArtifacts":
						_, err := d.StageArtifacts(ctx, &bazeloutputservice.StageArtifactsRequest{BuildId: "build"})
						return err
					default:
						_, err := d.BatchStat(ctx, &bazeloutputservice.BatchStatRequest{BuildId: "build"})
						return err
					}
				})
				requireBusyRPCPending(t, queued)
				release()
				require.NoError(t, <-started)
				require.NoError(t, <-queued)
			})
		})
	}
}

func TestBazelOutputServiceDirectoryBusyRestoreBuildIDCollision(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		a, b := mock.NewMockOutputPath(ctrl), mock.NewMockOutputPath(ctrl)
		a.EXPECT().FilterChildren(gomock.Any())
		b.EXPECT().FilterChildren(gomock.Any())
		entered, unblock := make(chan struct{}), make(chan struct{})
		release := sync.OnceFunc(func() { close(unblock) })
		defer release()
		d, _ := newBusyDirectory(t, busyOutputPathFactory{start: func(id path.Component) cd_vfs.OutputPath {
			if id.String() == "a" {
				close(entered)
				<-unblock
				return a
			}
			return b
		}})
		first := runBusyRPC(func() error { return startBusyBuild(ctx, d, "a", "shared") })
		<-entered
		require.NoError(t, startBusyBuild(ctx, d, "b", "shared"))
		release()
		require.Equal(t, codes.InvalidArgument, status.Code(<-first))
		require.Equal(t, codes.InvalidArgument, status.Code(startBusyBuild(ctx, d, "a", "shared")))
		require.NoError(t, startBusyBuild(ctx, d, "a", "next"))
		_, err := d.BatchStat(ctx, &bazeloutputservice.BatchStatRequest{BuildId: "shared"})
		require.NoError(t, err)
	})
}

func TestBazelOutputServiceDirectoryBusyDoesNotDrainAdmittedRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		a := mock.NewMockOutputPath(ctrl)
		a.EXPECT().FilterChildren(gomock.Any())
		a.EXPECT().FinalizeBuild(gomock.Any(), gomock.Any())
		entered, unblock := make(chan struct{}, 2), make(chan struct{})
		release := sync.OnceFunc(func() { close(unblock) })
		defer release()
		a.EXPECT().LookupChild(path.MustNewComponent("file")).DoAndReturn(func(path.Component) (re_vfs.PrepopulatedDirectoryChild, error) {
			entered <- struct{}{}
			<-unblock
			return re_vfs.PrepopulatedDirectoryChild{}, syscall.ENOENT
		})
		a.EXPECT().CreateAndEnterPrepopulatedDirectory(path.MustNewComponent("dir")).DoAndReturn(func(path.Component) (re_vfs.PrepopulatedDirectory, error) {
			entered <- struct{}{}
			<-unblock
			return nil, status.Error(codes.Internal, "create failed")
		})
		d, _ := newBusyDirectory(t, busyOutputPathFactory{start: func(path.Component) cd_vfs.OutputPath { return a }})
		require.NoError(t, startBusyBuild(ctx, d, "a", "build"))
		stat := runBusyRPC(func() error {
			_, err := d.BatchStat(ctx, &bazeloutputservice.BatchStatRequest{BuildId: "build", Paths: []string{"file"}})
			return err
		})
		stage := runBusyRPC(func() error {
			response, err := d.StageArtifacts(ctx, &bazeloutputservice.StageArtifactsRequest{BuildId: "build", Artifacts: []*bazeloutputservice.StageArtifactsRequest_Artifact{{Path: "dir/file"}}})
			if err != nil {
				return err
			}
			return status.FromProto(response.Responses[0].Status).Err()
		})
		synctest.Wait()
		require.Len(t, entered, 2, "staging and stat remain concurrent")
		// This ordering is outside the client's sequencing contract. Preserve
		// the old behavior rather than introducing full-RPC reader tracking.
		_, err := d.FinalizeBuild(ctx, &bazeloutputservice.FinalizeBuildRequest{BuildId: "build"})
		require.NoError(t, err)
		release()
		require.NoError(t, <-stat)
		require.Equal(t, codes.Internal, status.Code(<-stage))
	})
}

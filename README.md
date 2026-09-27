# gha-outrunner

[![CI](https://github.com/NetwindHQ/gha-outrunner/actions/workflows/ci.yml/badge.svg)](https://github.com/NetwindHQ/gha-outrunner/actions/workflows/ci.yml)
[![Release](https://github.com/NetwindHQ/gha-outrunner/actions/workflows/release.yml/badge.svg)](https://github.com/NetwindHQ/gha-outrunner/actions/workflows/release.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/NetwindHQ/gha-outrunner)](https://goreportcard.com/report/github.com/NetwindHQ/gha-outrunner)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Ephemeral GitHub Actions runners, no Kubernetes required.

![How gha-outrunner works](docs/info.png)

outrunner provisions fresh Docker containers or VMs for each GitHub Actions job, then destroys them when the job completes. It uses GitHub's [scaleset API](https://github.com/actions/scaleset) to register as an autoscaling runner group.

## Why outrunner?

GitHub's [Actions Runner Controller (ARC)](https://github.com/actions/actions-runner-controller) requires Kubernetes. If you're running on bare metal or a simple VPS, you shouldn't need a cluster just to get ephemeral runners. outrunner gives you the same isolation guarantees with Docker, libvirt, or Tart. No additional orchestrator needed.

Read more about [why outrunner](docs/explanation/why-outrunner.md), the [architecture](docs/explanation/architecture.md), and the [security model](docs/explanation/security.md).

## Provisioners

| Provisioner | Host OS | Runner OS | How it works |
|-------------|---------|-----------|--------------|
| Docker | Linux, macOS | Linux | Container per job. Fastest startup. |
| libvirt | Linux | Windows, Linux | KVM VM from qcow2 golden image with CoW overlays. QEMU Guest Agent for command execution. |
| Tart | macOS (Apple Silicon) | macOS, Linux (ARM64) | VM clone per job. Tart guest agent for command execution. |

See the [provisioner reference](docs/reference/provisioners.md) for lifecycle details and [runner image requirements](docs/reference/image-requirements.md) for what each backend expects.

## Get Started

Install outrunner for your platform:

- [Ubuntu / Debian](docs/setup/linux-deb.md)
- [Fedora / RHEL / CentOS](docs/setup/linux-rpm.md)
- [macOS (Homebrew)](docs/setup/macos.md)
- [From source](docs/setup/from-source.md)

Each guide gets you to a working Docker runner in minutes.

All packages and binaries are on the [Releases](https://github.com/NetwindHQ/gha-outrunner/releases) page.

## Going Further

### Other backends

- [Windows VMs via libvirt/KVM](docs/tutorial/libvirt-windows.md) - full VM isolation
- [macOS VMs via Tart](docs/tutorial/tart-macos.md) - Apple Silicon
- [Linux ARM64 VMs via Tart](docs/tutorial/tart-linux.md) - ARM64 Linux on Apple Silicon
- [More on Docker](docs/tutorial/docker.md) - custom images, socket detection, notes
- [Run multiple backends together](docs/howto/mixed-backends.md)

### Custom runner images

- [Docker](docs/howto/custom-docker-image.md)
- [Windows VM](docs/howto/custom-windows-image.md)
- [Tart macOS](docs/howto/custom-tart-macos-image.md)
- [Tart Linux](docs/howto/custom-tart-linux-image.md)


### Reference

- [CLI reference](docs/reference/cli.md)
- [Configuration reference](docs/reference/configuration.md)

## Used by

- [delo.so](https://delo.so) - Desktop, offline-first CAD for makers. Uses outrunner for CI, build and test pipelines.

Using outrunner? [Open a PR](https://github.com/NetwindHQ/gha-outrunner/edit/main/README.md) to add your project.

## Author

Built by [Paweł Subocz](https://x.com/psubocz) at [Netwind](https://netwind.pl).

### Docker exit recovery

A draining listener advertises zero capacity immediately while existing runners
finish, so it cannot acquire assignments that the scaler will refuse to launch.
Docker environments are retained until the scaler cleans them up. An exit watcher
inspects their state every two seconds, including exits that race startup. After
an exit, the scaler gives GitHub up to 30 seconds to deliver the real completion
message, then stops/removes the environment and deregisters the runner. It never
invents a job result. A failed stop/remove or deregistration cannot produce a
successful drain receipt. Failed starts also attempt teardown, since creation may
have succeeded before startup failed.

Docker logs are limited to two 1 MiB files. Before removal, the last 200 lines
(up to 64 KiB) are copied to private files under
`$XDG_CACHE_HOME/outrunner/diagnostics` (the latest 32 records across all scale sets). The systemd package
provides a writable cache directory. Exit codes and OOM status go to the journal;
job output remains in those private diagnostic files. Exited containers bearing this
scale set's ownership labels are cleaned up on restart. A surviving live container
blocks startup until its job exits; restart recovery does not stop it.

The credential-free integration test accepts `OUTRUNNER_EXIT_TEST_IMAGE`, pointing
to a local scratch image containing `provisioner/docker/testdata/exit` compiled
as `/exit`. Its `hold` argument supports the live-orphan restart test; otherwise
it exits with code 17. The tests verify immediate
exit detection, retained diagnostics, container removal, and repeated cleanup.

The grace window bounds recovery, so an unusually late GitHub completion may be
absent from the receipt and the observed completed-job count. Such nonempty,
untracked runner names produce a warning. Receipts report observed results;
GitHub remains the complete job history. Cleanup failures remain tracked and retry with exponential backoff while the
process runs. Shutdown without proven cleanup fails closed; it cannot produce
a receipt.

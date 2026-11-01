# discord-golive-relay

[![tests](https://github.com/datpham17903/discord-golive-relay/actions/workflows/tests.yml/badge.svg)](https://github.com/datpham17903/discord-golive-relay/actions/workflows/tests.yml)

Relay one consenting person's Discord Go Live to one or more relay accounts: watch once, encode once, publish many times.

Step-by-step guide to install, validate and run the relay. Comments in the code cite sections of the design specification as `spec §N`; the specification itself is not part of this repository.

## 1. What it is

One consenting person (the target) shares their screen with Discord Go Live in one voice channel. A relay account joins the same channel, watches that stream, re-encodes it once, and publishes it again as its own Go Live. Extra publish-only accounts can publish the same encoded video, so more people can watch (up to 50 viewers per Go Live).

```
target Go Live
      |
      v
ingest relay account (watches the target, uses 1 viewer slot)
      |
      v
one decode + one encode
      |
      +--> relay Go Live (ingest account)   up to 50 viewers
      +--> relay Go Live (output account 1) up to 50 viewers
      +--> ... up to 9 extra output accounts
```

The relay stops by itself when the target stops sharing, leaves the voice channel or moves to another channel.

## 2. Important warnings

- Automating a normal Discord account (a "self-bot") violates Discord's terms. Accounts can be terminated. Every extra account adds risk. Use dedicated accounts, never your main one.
- You need the clear consent of the person being relayed, for this destination and this audience. Write down the agreement and put a reference in `authorization.agreement_ref`. The config field does not replace real consent.
- Never share a token and never commit one. Tokens live only in token files (`secrets/` is gitignored). If a token was pasted in a chat or log, log out of that account or change its password to invalidate it, then get a new token.
- Never use the target's own password or token.

## 3. Requirements and install

| Item | Need |
| --- | --- |
| OS | Windows 10/11, Linux (VPS target), macOS (encoders only, not tested live) |
| Python | 3.10 or newer (developed on 3.14) |
| Network | Outbound UDP and TCP to Discord (voice uses UDP) |
| GPU | Optional. libx264 (CPU) always works |

Install from the project folder. Use the exact pinned versions: `discord.py-self` is pinned by git commit `2ba64a9a997e151a9c259984e0a179b1fdf4aff4`, `discord-native-voice` 0.1.1 is installed with `--no-deps`, `av` is 19.0.2 (all in `manifests/requirements.lock.txt`; the `git` command must be installed).

Linux:

```bash
python3 -m venv .venv
.venv/bin/pip install -r manifests/requirements.lock.txt
.venv/bin/pip install --no-deps discord-native-voice==0.1.1
.venv/bin/pip install -e ".[test]"
```

Windows (PowerShell):

```powershell
python -m venv .venv
.venv\Scripts\pip install -r manifests/requirements.lock.txt
.venv\Scripts\pip install --no-deps discord-native-voice==0.1.1
.venv\Scripts\pip install -e ".[test]"
```

Why `--no-deps`: the wheel asks for `discord.py-self>=2.2.0`, which the pinned 2.2.0a build does not satisfy by version number.

Check the install (see section 7 for `doctor`):

```bash
.venv/bin/relayctl doctor --config config.example.yaml     # Windows: .venv\Scripts\relayctl
```

You should see `[ok] import av`, `[ok] import discord`, `[ok] import yaml` and at least `[ok] encoder probe libx264`. With the example config, `doctor` also reports `capability` failures. That is expected until section 6 is done, and `doctor` then exits with code 1. On Windows, set `storage.state_directory` first (section 5), because `doctor` creates that directory.

If `relayctl` is not found, re-run `pip install -e ".[test]"`.

Commands below write `relayctl` and `python`. Activate the venv first (`source .venv/bin/activate` or `.venv\Scripts\Activate.ps1`) or use the full `.venv` path.

## 4. Getting IDs and tokens

IDs (all are long numbers, always written as quoted strings):

1. Discord: User Settings, Advanced, turn on Developer Mode.
2. Right-click a user, channel or server and choose Copy User ID / Copy Channel ID / Copy Server ID.

| You need | Where |
| --- | --- |
| `guild_id` | the server |
| `target_user_id` | the person being relayed |
| `allowed_voice_channel_id` | the voice channel they share in |
| `expected_relay_user_id` | the ingest relay account |
| each output account `user_id` | each extra relay account |

Tokens: use a dedicated relay account per token. Log in to Discord in a browser, open developer tools (F12), Network tab, click any request to `discord.com/api`, and copy the `authorization` request header value. Do not log out with the Discord "Log out" button afterwards, because that invalidates the token.

Store each token as a one-line text file (name = credential name):

| File | Used by |
| --- | --- |
| `secrets/relay-token` | `relayctl run`, ingest account (`discord.ingest_credential`) |
| `secrets/relay-token-2`, ... | `relayctl run`, output accounts (`credential` in `output_accounts`) |
| `secrets/relay.token`, `secrets/relay2.token` | M0 spikes (paths set in `spikes/m0/m0.json`) |

On Linux restrict them: `chmod 600 secrets/*`. The relay never reads tokens from YAML or the command line.

## 5. Configuration

Copy the example and edit it. `config.yaml` is gitignored.

```bash
cp config.example.yaml config.yaml          # Windows: Copy-Item config.example.yaml config.yaml
relayctl validate --config config.yaml
```

`validate` prints every problem at once. Unknown keys are errors. All IDs must be quoted strings. The file is read-only for the relay: it never writes YAML (except `calibrate_tdead.py --write`, section 6).

### Keys

| Key | Meaning | Default in example | Change when |
| --- | --- | --- | --- |
| `schema_version` | Config format | `1` | never |
| `discord.expected_relay_user_id` | ID of the ingest account | dummy | always |
| `discord.guild_id` | Server ID | dummy | always |
| `discord.target_user_id` | Person to relay (must differ from the relay) | dummy | always |
| `discord.allowed_voice_channel_id` | The only voice channel used | dummy | always |
| `discord.ingest_credential` | Token file name of the ingest account | `relay-token` | you use another file name |
| `discord.ingest_publishes` | Ingest account also publishes its own Go Live | `true` | set `false` to publish only from output accounts |
| `discord.output_accounts` | Extra publish-only accounts, max 9 (list of `{user_id, credential}`) | `[]` | you want more relay Go Lives |
| `authorization.agreement_ref` | Note pointing to the consent record | placeholder | always |
| `authorization.excluded_viewer_ids` | Viewer IDs the target asked to exclude | `[]` | the target asks |
| `codecs.parent_codecs`, `watch_codecs` | Codec negotiation mode | `auto_decode` | never (only value) |
| `codecs.output_codec` | Output codec | `H264` | never (only value) |
| `codecs.output_rids` | Output layers | `1` | never (only 1 supported) |
| `codecs.video_encoder` | `auto`, `libx264`, `h264_nvenc`, `h264_amf`, `h264_qsv`, `h264_mf`, `h264_videotoolbox` (`h264_vaapi` is accepted but not usable) | `auto` | force a specific encoder (section 9) |
| `codecs.hw_decode` | `none`, `auto`, `d3d11va`, `cuda`, `vaapi`, `videotoolbox` | `none` | see section 9 |
| `quality.mode` | `auto` (use `max_*`) or `fixed` (use `fixed_*`) | `auto` | you need exact size/fps |
| `quality.max_width/height/fps` | Output cap in `auto` mode (width and height must be even) | 1920 x 1080 @ 60 | lower for weak CPU or network |
| `quality.fixed_width/height/fps` | Output in `fixed` mode | 1920 x 1080 @ 60 | only with `fixed` |
| `quality.video_bitrate_kbps` | Encode bitrate per output | `8000` | lower if you see error 2003 (section 10) |
| `quality.audio_bitrate_kbps` | Audio bitrate | `128` | rarely |
| `quality.approved_profile_manifest` | Approved profiles file | `manifests/verified-profiles.json` | different location |
| `audio.enabled` | Relay the stream audio | `true` | set `false` for video only |
| `audio.source` | Audio source | `target_stream_only` | never (only value) |
| `runtime.start_debounce_ms` | Wait before reacting to a start | `500` | rarely |
| `runtime.idle_voice_grace_seconds` | Grace while the target is idle in voice | `30` | rarely |
| `runtime.hold_recovery_timeout_seconds` | How long a HOLD (slate) may last | `15` | rarely |
| `runtime.connect_timeout_seconds` | Voice connect timeout | `15` | slow networks |
| `runtime.first_media_timeout_seconds` | Wait for first media from the target | `10` | rarely |
| `runtime.retry_delays_seconds` | Backoff list, then a cooldown | `[1, 2, 4, 8, 15]` | rarely |
| `runtime.retry_cooldown_seconds` | Pause after the list is used up | `60` | rarely |
| `runtime.keyframe_interval_seconds` | Keyframe interval (must be > 0 and <= 2) | `2` | rarely |
| `runtime.feedback_idr_min_interval_ms` | Min gap between keyframes forced by viewer feedback | `500` | rarely |
| `runtime.repeated_rejection_limit` | Consecutive rejected sessions before FATAL | `3` | rarely |
| `runtime.recording_enabled` | Recording | `false` | never (must be `false`) |
| `liveness.source_dead_timeout_seconds` | Seconds without source packets before the relay treats the source as dead (`T_dead`) | `null` | set by section 6 (the service refuses to run while `null`) |
| `liveness.verified_capability_manifest` | Capability file with the measured value | `manifests/verified-capabilities.json` | different location |
| `storage.state_directory` | Where `state.json`, `status.json` and the control socket live | `/var/lib/discord-relay` | always on Windows (for example `state`) |
| `logging.level` | `DEBUG` `INFO` `WARNING` `ERROR` `CRITICAL` | `INFO` | debugging |
| `logging.include_media` | Log media content | `false` | never (must be `false`) |

Relative manifest paths are looked up next to the config file.

### Multiple accounts

Each output account needs a Discord user ID and its own token file. The credential name must match `[A-Za-z0-9_.-]+` and be unique (also different from `ingest_credential`).

```yaml
discord:
  ingest_credential: relay-token
  output_accounts:
    - {user_id: "111111111111111111", credential: relay-token-2}
    - {user_id: "222222222222222222", credential: relay-token-3}
```

Token files: `relay-token`, `relay-token-2`, `relay-token-3`. Each account has its own quarantine and FATAL latch; one failing output does not stop the others. Output accounts do not watch the target, so they use no extra viewer slot on the target's stream.

### Approved profile manifest

The service only runs a quality profile that was verified live on that exact relay account and server. Edit `manifests/verified-profiles.json` (gitignored; template: `manifests/verified-profiles.example.json`):

```json
{
  "schema_version": 1,
  "profiles": [
    {
      "relay_user_id": "YOUR_INGEST_ACCOUNT_ID",
      "guild_id": "YOUR_GUILD_ID",
      "width": 1920,
      "height": 1080,
      "fps": 60,
      "verified_on": "2026-11-01",
      "evidence": "G2b run, viewer saw 1080P 60FPS and smooth video"
    }
  ]
}
```

`width`, `height`, `fps` must equal `max_*` (auto) or `fixed_*` (fixed). Add the entry after the profile played well in the G2b gate (section 6). `verified-profiles.json` is local (gitignored) and holds the owner's IDs, so copy `verified-profiles.example.json` and edit it with your own account and server.

## 6. First-time validation (M0 gates)

Run these once, in order, before enabling the service. The spikes are standalone scripts that use a small JSON file, not the YAML config. Each run writes a log `spikes/m0/out/<gate>-<timestamp>.jsonl` (tokens are redacted). Stop any spike with Ctrl+C, or create the file `spikes/m0/out/STOP` (G1 uses `<its log name>.STOP`). If G2a or G2b fails, stop and fix the protocol problem first.

Prepare once:

```bash
cp spikes/m0/m0.example.json spikes/m0/m0.json    # Windows: Copy-Item
```

Edit `spikes/m0/m0.json`: `guild_id`, `target_user_id`, `voice_channel_id`, `expected_relay_user_id`, `token_file` (relative to the JSON file) and, for G2c, `output_accounts` as `{"user_id", "token_file"}`. Remove the `output_accounts` entry if you do not test G2c. Put the tokens in `secrets/`.

All commands below run from the project folder.

### Checklist

| Gate | Command | What you do in Discord | Success looks like (in the log or console) |
| --- | --- | --- | --- |
| G1 probe | `python spikes/m0/g1_probe.py --config spikes/m0/m0.json --minutes 10` | Relay account is online but not in voice. The target joins voice, starts and stops sharing, leaves. | `VOICE_STATE_UPDATE` events for the target, including `self_stream` changes. |
| G2a | `python spikes/m0/g2a_watch_and_publish.py --config spikes/m0/m0.json --minutes 5` | The target joins the voice channel and starts sharing. A second person checks the channel. | Events `watch_connected` and `output_connected`; viewer sees a test pattern Go Live next to the target's; `session_end` is clean. |
| G2b | `python spikes/m0/g2b_relay.py --config spikes/m0/m0.json --minutes 30` | The target shares real content (with sound) for the whole 30 minutes. A viewer opens the relay Go Live. | `output_playing`; viewer sees smooth video and sound; no `output_play_ended` with an error; 30 minutes without restart. |
| G2c | `python spikes/m0/g2c_multi.py --config spikes/m0/m0.json --minutes 30` | Same as G2b, with `output_accounts` filled in. Open each relay Go Live. | `connected` and `playing` events for every account; viewers see all relay streams; only the ingest account is in the target's viewer list. |
| T_dead | see below | Target shares a static screen for 10 minutes (no movement). | the tool prints a recommended value. |
| G4 | Run G2b or G2c again (same commands) | While it runs, let viewers join and leave the relay Go Live several times. | Stream stays up; DAVE state in `stats` events does not report errors; no decode errors. |

Useful spike options (all optional): `--max-width`, `--max-height`, `--fps`, `--video-kbps` (default 8000), `--encoder` (`auto`, `x264`, `amf`, `h264_nvenc`, ...; default `x264` in G2b, `auto` in G2c), `--hw-decode`, `--audio-kbps`, `--pacing` (`cfr` or `source`, default `source`), `--jitter-ms`, `--jitter-max-ms` (default 600), `--source-dead-seconds`. G2a also takes `--width --height --fps` of its test pattern. Run `python spikes/m0/g2b_relay.py --help` for the full list.

### T_dead calibration

1. Run G2c (or G2b) while the target shows a static screen for at least 10 minutes.
2. Dry run (prints the recommended value, writes nothing):

   ```bash
   python tools/calibrate_tdead.py spikes/m0/out/g2c-*.jsonl
   ```

   On Windows PowerShell, pass the files explicitly, for example `python tools/calibrate_tdead.py (Get-ChildItem spikes/m0/out/g2c-*.jsonl)`.
3. Write the value into the capability manifest and your config:

   ```bash
   python tools/calibrate_tdead.py spikes/m0/out/g2c-*.jsonl --write --config config.yaml
   ```

   The tool refuses with less than 10 minutes of watched source (`--min-minutes`, or `--force`). The formula is `max(3.0, round_up(2 x observed_max_gap + 1.0, 0.5))`. By default `--write` updates `manifests/verified-capabilities.json` in the project folder; use `--manifest PATH` for another copy.

### Where results are recorded

`manifests/verified-capabilities.json`: `live.source_dead_timeout_seconds` (written by the tool) and the notes under `live_results` (G1, G2a, G2b, G2c, T_dead). Edit the notes by hand after each gate. `calibrate_tdead.py --write` adds `live_results.T_dead`. Keep the same value in `config.yaml`. Also add the profile to `verified-profiles.json` (section 5).

## 7. Running the service

Tell the service where the tokens are:

| Where | How |
| --- | --- |
| Development | `RELAY_TOKEN_DIR=secrets` (files named by credential). `RELAY_TOKEN_FILE=path` also works but only for the ingest account. |
| systemd | `LoadCredential=` (section 8) sets `CREDENTIALS_DIRECTORY` |

```bash
export RELAY_TOKEN_DIR=secrets              # Windows PowerShell: $env:RELAY_TOKEN_DIR = "secrets"
relayctl validate --config config.yaml
relayctl doctor   --config config.yaml
relayctl run      --config config.yaml
```

| Command | What it does | Output |
| --- | --- | --- |
| `relayctl validate --config FILE` | Checks the config and approved profile. No Discord connection. | `config valid`, then one `not operable: ...` line per reason the service would refuse to run (null `T_dead`, profile not approved). Exit 1 and the list of errors if the config is invalid. |
| `relayctl doctor --config FILE` | Checks imports, encoder and decoder probes, state directory, capability. Never publishes. | Lines starting with `[ok]`, `[no]` (probe not available, fine) or `[FAIL]`. Exit 1 if any `[FAIL]`. |
| `relayctl run --config FILE` | Runs the service. Exit 78 means a config or credential problem (no automatic restart under systemd). | Starts disabled on first run (see below). |
| `relayctl status [--state-dir DIR]` | Shows state, enabled flag, FATAL, quarantine, profile. Asks the running service, else reads `status.json` or `state.json`. | JSON. The `source` field says where it came from. |
| `relayctl enable [--state-dir DIR]` | Sets `desired_enabled=true`. Does not clear FATAL or quarantine. | `{"ok": true, ...}` |
| `relayctl disable [--state-dir DIR]` | Sets `desired_enabled=false` and stops the session. Survives restarts. | `{"ok": true, ...}` |
| `relayctl reset-fatal [--state-dir DIR] [--account USER_ID]` | Clears a FATAL latch after you fixed the cause. Without `--account` it clears the ingest account; with it, that output account's latch. Does not enable and does not clear quarantine. | `no fatal latch set` or the new state. |

`--state-dir` must equal `storage.state_directory` (default `/var/lib/discord-relay`). Examples:

```bash
relayctl enable      --state-dir /var/lib/discord-relay
relayctl status      --state-dir /var/lib/discord-relay
relayctl reset-fatal --state-dir /var/lib/discord-relay --account 111111111111111111
```

A fresh install is disabled. Run `relayctl enable` once. The service then relays automatically whenever the target shares in the allowed voice channel. It stops when the target stops sharing, leaves or moves, or when you run `relayctl disable` (or SIGINT/SIGTERM). Runtime state (enabled flag, FATAL latch, quarantine, rejection counter) is in `state.json` inside the state directory; the config is never modified.

Windows note: the control socket is a Unix socket. If it is unavailable, `enable`, `disable` and `reset-fatal` change `state.json` directly and `status` reads the files, so run them while the service is stopped or after it exited.

## 8. Deploying on a Linux VPS with systemd

Sizing (measured on the owner's laptop and in the G2b/G2c runs):

| Item | Guide |
| --- | --- |
| CPU | 4 vCPU for libx264 1080p60 (about 160% of one core measured for the worker). A GPU is optional. |
| Bandwidth | Download: the target's stream (about 1 to 8 Mbps). Upload: about 8 Mbps per publishing account (default bitrate). Viewers do not multiply upload because they connect to Discord. |
| RAM | 4 to 8 GB is a safe start. |
| Placement | Not on the target's streaming PC: its uplink is shared and caused Discord errors 2002/2003. |

Steps (as root):

```bash
useradd --system --home /opt/discord-relay --shell /usr/sbin/nologin discord-relay
git clone https://github.com/datpham17903/discord-golive-relay.git /opt/discord-relay && cd /opt/discord-relay
python3 -m venv .venv
.venv/bin/pip install -r manifests/requirements.lock.txt
.venv/bin/pip install --no-deps discord-native-voice==0.1.1
.venv/bin/pip install -e .

install -d -m 0750 -o root -g discord-relay /etc/discord-relay /etc/discord-relay/manifests
install -m 0640 -g discord-relay config.yaml /etc/discord-relay/config.yaml
install -m 0640 -g discord-relay manifests/verified-capabilities.json manifests/verified-profiles.json /etc/discord-relay/manifests/
install -m 0600 secrets/relay-token   /etc/discord-relay/token
install -m 0600 secrets/relay-token-2 /etc/discord-relay/token-2
```

The manifests go in `/etc/discord-relay/manifests/` because relative paths in the config are resolved next to the config file. The config should keep `storage.state_directory: /var/lib/discord-relay` (the unit's `StateDirectory=discord-relay`).

Edit `deploy/systemd/discord-relay.service`: it has one `LoadCredential` line for the ingest token. Add one line per output account, with the credential name from `output_accounts` on the left:

```
LoadCredential=relay-token:/etc/discord-relay/token
LoadCredential=relay-token-2:/etc/discord-relay/token-2
```

The unit already sets: user `discord-relay`, `Restart=on-failure`, `RestartPreventExitStatus=78`, `TimeoutStopSec=20`, `LimitCORE=0` (no core dumps), `ReadOnlyPaths=/etc/discord-relay`, `NoNewPrivileges=yes`. Check that `ExecStart` points to your venv (`/opt/discord-relay/.venv/bin/relayctl`).

```bash
install -m 0644 deploy/systemd/discord-relay.service /etc/systemd/system/
systemctl daemon-reload
/opt/discord-relay/.venv/bin/relayctl doctor --config /etc/discord-relay/config.yaml
systemctl enable --now discord-relay
sudo -u discord-relay /opt/discord-relay/.venv/bin/relayctl enable    # first run is disabled
journalctl -u discord-relay -f
```

Run `relayctl doctor` as the service user if you hit permission errors. Run one instance per account. Do not use the same account in another Discord client at the same time.

## 9. Hardware acceleration

`codecs.video_encoder: auto` uses the first encoder that passes a probe, in this order:

| Platform | Auto order |
| --- | --- |
| Windows | `h264_nvenc`, `h264_amf`, `h264_qsv`, `h264_mf`, `libx264` |
| Linux | `h264_nvenc`, `h264_qsv`, `h264_amf`, `libx264` |
| macOS | `h264_videotoolbox`, `libx264` |

`relayctl doctor` shows what works on this machine:

```
[ok] encoder probe libx264: delay 0 frames, p50 ... ms
[no] encoder probe h264_nvenc: <reason>
[ok] decode device d3d11va
auto decode (h264): ...
[ok] encoder auto -> h264_amf
```

`[no]` just means that encoder is not usable here. The last line (`encoder <setting> -> <name>`) is what the service will use. The probe checks that the encoder starts and that output delay is acceptable.

To force one, set for example `video_encoder: h264_nvenc`. If it is unavailable, `doctor` reports `[FAIL]` and the service does not fall back to another encoder (it fails closed). `h264_vaapi` is accepted by the config but is not supported by the pipeline.

Decoding: `hw_decode: none` (software) is the default. On the owner's integrated GPU (Radeon 780M) that was also encoding, `d3d11va` decode raised relay latency from about 80 ms to about 240 ms. Try `auto` or a device (`d3d11va`, `cuda`, `vaapi`, `videotoolbox`) only on a machine with a free GPU, and compare latency.

Benchmark encoders with synthetic frames (nothing is written to disk):

```bash
python tools/bench_encode.py --seconds 10
python tools/bench_encode.py --profile motion      # profiles: saver, default, detail, motion
```

It prints p50/p95 encode time, CPU time and memory growth per profile.

## 10. Tuning and troubleshooting

### Smoothness versus latency

The relay buffers incoming video in an adaptive jitter buffer, up to 600 ms, because source packets arrive in bursts. Audio is delayed to follow the video. Expect about 0.3 to 0.6 s end-to-end latency in G2c, and 70 to 130 ms inside the pipeline. The 600 ms cap is not a config key in the service; the spikes accept `--jitter-ms` and `--jitter-max-ms` for experiments.

### Discord errors 2002 / 2003 (reduced quality / bandwidth)

- Run the relay on a machine that is not the target's streaming PC (separate uplink).
- Lower `quality.video_bitrate_kbps`. 20000 caused 2003 on a shared uplink; 8000 was fine. Budget about 8 Mbps per publishing account.
- Fewer output accounts on one connection.

### Stutter checklist

1. Check upload: total = bitrate x number of publishing accounts, plus the target's own stream if on the same line.
2. Run `relayctl doctor`: is a software encoder at 1080p60 too slow? Lower `max_fps` or resolution, or use a hardware encoder.
3. Keep `hw_decode: none` unless measured otherwise.
4. Stalls hitting every account at the same moment come from the source arriving late (target's upload), not from the relay.

### FATAL latches and quarantine

| Item | Meaning | What to do |
| --- | --- | --- |
| FATAL `AUTH_REJECTED` | Discord rejected the token | Get a new token, replace the file, `reset-fatal` |
| FATAL `ACCOUNT_OWNERSHIP_UNCERTAIN` | The account's voice session changed (probably used elsewhere) | Make sure nothing else uses the account, `reset-fatal` |
| FATAL `RELAY_REJECTED_BY_SERVER` | 3 relay sessions in a row were kicked or moved | Check channel permissions and moderators, `reset-fatal` |
| QUARANTINE | The account was moved or disconnected. It will not rejoin until the target's current share ends and a new one starts. | Wait for the next share. `disable`/`enable` do not clear it. |

After fixing the cause: `relayctl reset-fatal --state-dir DIR` (add `--account ID` for an output account), then `relayctl enable` if needed. `reset-fatal` never enables the relay by itself.

### Common config errors

| Message | Fix |
| --- | --- |
| `x.y: missing` / `unknown key` | Compare with `config.example.yaml`; names are exact |
| `must be a string of decimal digits` | Quote the ID: `"123..."` |
| `must be even` | `max_width` and `max_height` must be even numbers |
| `recording_enabled: must be false (not implemented)` | Leave it `false` (same for `include_media`) |
| `not operable: liveness.source_dead_timeout_seconds ...` | Do the T_dead calibration (section 6) |
| `not operable: profile ... not approved` | Add the profile to `verified-profiles.json` (section 5) |
| `output_accounts[i]: ...` | Need exactly `user_id` and `credential`; IDs and credential names must be unique |
| `no credential: ...` | Set `RELAY_TOKEN_DIR` (or systemd `LoadCredential`); file name must equal the credential name |

## 11. What is verified live

From `live_results` in `manifests/verified-capabilities.json` (all on the owner's Windows laptop, 2026-11-01):

| Item | Status |
| --- | --- |
| G1 detect start/stop without joining voice | Pass (detection through `self_stream` only) |
| G2a watch and publish with one account (DAVE) | Pass |
| G2b relay of real content (1080p60, 8 Mbps, audio) | Partial: smooth 1080p60 confirmed; longest run 15.4 min, no 30-minute run yet |
| G2c two accounts (ingest plus one output) | Pass; 2 relay accounts only, error 2002 on the shared uplink |
| `T_dead` (`source_dead_timeout_seconds`) | Open (`null`) |
| 30-minute G2b run | Open |
| G4 viewers joining and leaving, 50 viewers | Open |
| `relayctl run` as a service on a Linux VPS | Open |
| More than 2 relay accounts, quarantine on kick/move live | Open |

## 12. Project layout and development

```
config.example.yaml       example config (copy to config.yaml)
relay/                    the service
  config.py               config loader and validator
  service.py              wires everything together (relayctl run)
  adapter/                Discord access (watching, publishing, output accounts)
  control/                relayctl, state.json, control socket, instance lock
  media/                  decode, encode (hwaccel.py), publisher, slate
  supervisor/             state machine, liveness, ownership, retry, outputs
  worker/                 media worker process (process.py, messages.py, canvas, timeline, IPC)
spikes/m0/                live gate scripts (G1, G2a, G2b, G2c), helpers (common.py, session.py,
                          relay_worker.py; `python spikes/m0/relay_worker.py --selftest`), m0.example.json, out/ logs
tools/                    bench_encode.py, calibrate_tdead.py
manifests/                requirements.lock.txt, build-manifest.json (exact versions),
                          verified-capabilities.json (live results, T_dead),
                          verified-profiles.json (+ .example.json)
deploy/systemd/           discord-relay.service
tests/                    pytest suite
.github/workflows/        CI: test suite on Linux
secrets/                  token files (gitignored)
```

Run the tests (no Discord needed):

```bash
python -m pytest -q
```

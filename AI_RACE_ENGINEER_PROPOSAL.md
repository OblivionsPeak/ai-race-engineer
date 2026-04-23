# AI Race Engineer
### Product Proposal — OpMo eSports

---

## Executive Summary

AI Race Engineer is a real-time, voice-first AI co-pilot for iRacing endurance teams. It listens to the driver, reads live telemetry, and delivers strategic guidance the way a real race engineer would — concisely, in the moment, without the driver taking their eyes off the road. Built on Claude AI and integrated with pyirsdk, it closes the gap between data-rich iRacing telemetry and the split-second decisions that win endurance races.

---

## The Problem

Endurance racing in iRacing is a team sport, but most teams manage strategy with spreadsheets, Discord threads, and gut feel. Even teams using dedicated strategy tools face a core bottleneck:

- The driver is heads-down. They can't read a dashboard mid-stint.
- The spotter/engineer is often the same person doing four other jobs.
- Fuel, tire, and position decisions compound each other in non-obvious ways.
- Incidents, yellows, and competitor pit windows require real-time recalculation.

The result: teams leave time on track, run out of fuel, or make suboptimal pit calls — not from lack of effort, but from cognitive overload.

---

## The Solution

A voice-activated AI race engineer that runs alongside iRacing, reads live telemetry, knows the race plan, and answers natural language questions from the driver or crew chief in real time.

**Driver asks:** *"How's the fuel looking?"*  
**AI answers:** *"You're 0.3 litres per lap under target. At this rate you can extend your stint by 2 laps. Current exit window is lap 34, but you have margin to go to lap 36."*

**Driver asks:** *"Car 14 just pitted. Do we stay out?"*  
**AI answers:** *"Car 14 is now 2 laps behind you on strategy. If you stay out to lap 38 you come out ahead by 4 seconds. Recommend staying out."*

No screens. No Discord tab. No mental arithmetic. Just answers.

---

## Core Features

### 1. Voice Query Interface
- Push-to-talk or always-on wake word ("Hey Engineer")
- Whisper-based speech-to-text (runs locally or via API)
- Text-to-speech response via OS native TTS or ElevenLabs for realism
- Response target: under 2 seconds from question to answer

### 2. Live Telemetry Context
Reads from iRacing via pyirsdk every second:
- Current lap, fuel level, stint time elapsed
- Car position, gap to cars ahead/behind
- Tire age, track conditions
- Competitor pit road status and lap deltas

### 3. Race Plan Awareness
Ingests the team's active race plan from the Endurance Strategy Planner:
- Stint schedule and driver rotation
- Target fuel per lap, planned pit laps
- Contingency plans (safety car, rain, early damage)

### 4. Strategic Intelligence Modes

| Mode | Trigger | Model | Latency |
|---|---|---|---|
| Quick Answer | Routine query ("fuel?", "gap?") | Claude Haiku 4.5 | ~1s |
| Strategic Analysis | Complex decision ("should we two-stop?") | Claude Sonnet 4.6 | ~3s |
| Incident Response | Yellow flag, competitor crash | Haiku (auto-triggered) | ~1.5s |

### 5. Proactive Alerts
Engineer speaks up without being asked:
- "Fuel warning — 3 laps to pit window"
- "Car 7 is closing at 1.2 seconds per lap. Pit window overlap in 4 laps"
- "You're pacing 0.5s off your target lap time. Sector 2 looks like the loss"

### 6. Post-Race Debrief
After the session ends, generates a written debrief:
- Stint-by-stint fuel efficiency vs. plan
- Pit stop timing accuracy
- Position changes attributed to strategy vs. pace
- Recommendations for next race

---

## Technical Architecture

```
iRacing (pyirsdk)
      │
      ▼
Telemetry Bridge (Python)
  - Reads IR data every 1s
  - Maintains rolling state
  - Detects events (pit entry/exit, yellow, incident)
      │
      ▼
AI Engineer Core (Python)
  - Listens for voice input (Whisper)
  - Assembles context snapshot (~1,500 tokens)
  - Calls Claude API (Haiku or Sonnet)
  - Reads response via TTS
      │
      ▼
Claude API
  - System prompt: race engineer persona + race plan
  - User message: telemetry snapshot + driver question
  - Prompt cache: static race plan context (5-min TTL)
      │
      ▼
Endurance Strategy Planner (existing)
  - Source of truth for race plan, stints, contingencies
  - REST API: /api/plans/{id} feeds the AI context
```

**Desktop app** (Electron or standalone Python + tray icon) so it runs alongside iRacing without a browser tab.

---

## Cost Analysis

### Per-Race API Costs

| Usage | Tokens | Cost |
|---|---|---|
| Race plan context (cached) | ~1,200 input | ~$0.001 (cached rate) |
| Telemetry snapshot per query | ~300 input | ~$0.0002 |
| Response | ~150 output | ~$0.0003 |
| **20 queries per race** | — | **~$0.02** |
| 24h endurance (1 query/5min) | — | ~$0.30 |
| Whisper transcription (2hr race) | ~30min audio | ~$0.18 |
| **Total per race (typical)** | — | **< $0.25** |

### Business Model

| Tier | Price | Target User |
|---|---|---|
| Solo | $9/month | Individual driver |
| Team | $24/month | Up to 6 team members |
| League | $79/month | League operators, multiple teams |

At $9/month solo, breakeven at ~36 races/month — a level no individual user reaches. Margins are healthy from the first paying subscriber.

---

## Competitive Advantage

No direct competitor exists in the iRacing space. The adjacent landscape:

- **MoTeC / Atlas** — post-session data analysis, not real-time, no natural language
- **Crew Chief** (existing iRacing spotter app) — lap time deltas and gap calls, no strategy intelligence, no AI
- **OpenAI + iRacing hobbyist projects** — proof-of-concept demos, not productized

The moat is the combination: voice interface + live telemetry + team race plan awareness + endurance-specific strategic reasoning. Crew Chief handles the spotter layer. AI Race Engineer handles the engineering layer.

---

## Integration with Endurance Strategy Planner

AI Race Engineer is not a standalone product — it's the intelligence layer on top of the planner already built:

- Reads active plan via existing REST API
- Pushes post-race debrief back to planner database
- Shares team auth (same invite code, same login)
- Pit wall display shows when AI engineer is "listening"

This makes the endurance planner the hub of a two-product suite, increasing retention for both.

---

## Development Roadmap

### Phase 1 — Local MVP (4–6 weeks)
- [ ] Voice capture + Whisper transcription (push-to-talk)
- [ ] Context assembler: telemetry snapshot + plan fetch
- [ ] Claude Haiku integration with race engineer system prompt
- [ ] TTS response playback
- [ ] Basic proactive alerts (fuel warning, pit window)
- [ ] Windows desktop tray app

### Phase 2 — Intelligence Layer (4–6 weeks)
- [ ] Competitor tracking in context ("Car 7 is on lap 22, last pitted lap 8")
- [ ] Yellow flag strategy mode ("do we pit under yellow?")
- [ ] Sonnet escalation for complex multi-variable decisions
- [ ] Post-race debrief generation
- [ ] Wake word support

### Phase 3 — Product Polish (2–3 weeks)
- [ ] ElevenLabs voice (custom engineer persona)
- [ ] Subscription billing (Stripe)
- [ ] League admin dashboard
- [ ] Public launch

**Total estimated build time: 10–15 weeks solo, 6–8 weeks with one additional developer.**

---

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| iRacing API changes breaking telemetry | pyirsdk is community-maintained and stable; pin version |
| Latency too high for racing use | Haiku is fast; pre-warm API connection at race start |
| Voice recognition errors in noisy environments | Show text of what AI heard before speaking response |
| Users abusing API (excessive queries) | Rate limit: 1 query per 20 seconds; daily cap per tier |
| ElevenLabs cost at scale | Ship with OS TTS by default; ElevenLabs as premium add-on |

---

## Why Now

- iRacing endurance racing is growing — 24h Nürburgring, Daytona, Le Mans series all have hundreds of team entries
- Claude Haiku 4.5 is fast and cheap enough to make real-time voice viable for the first time
- The Endurance Strategy Planner already exists as the foundation — this is an extension, not a greenfield build
- No one has shipped a polished version of this yet

The window to be first is open.

---

*Prepared by OpMo eSports — April 2026*

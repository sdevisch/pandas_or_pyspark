# Repository Clarity Plan

Goals
- Reduce cognitive load: clear entry points, fewer overlapping scripts, consistent names
- Separate concerns: measurement vs reporting vs demos
- Make front-ends vs backends explicit in docs and code

Actions
1) Rename reports and scripts (done)
   - brc_1b_groupby.md, brc_smoke_groupby.md, brc_order_of_magnitude.md, brc_under_1min_capacity.md
   - brc_jsonl_pipeline orchestrator; JSONL-first reporting
2) Consolidate docs
   - docs/README.md as index; keep README.md focused
   - Move plans here; remove legacy *_PLAN.md from root
3) Front-ends exploration
   - perfcore/frontends registry; enhance measure CLI help
   - Document how to add a new frontend adapter
4) Demos
   - Remove demo_api_3_min; recommend running individual scripts or `scripts/api_demo/demo_runner.py`
5) Housekeeping
   - Ensure default outputs don’t overwrite main BRC unless 1B
   - Route small runs to smoke

Milestones
- M1: Docs and names aligned (this branch)
- M2: Demo and perf tooling cohesive walkthrough
- M3: Optional: consolidate runners under a single CLI facade

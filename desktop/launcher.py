"""Entry point of the packaged backend. A worker process of the same program starts with "__slot__"."""
import sys

if len(sys.argv) > 1 and sys.argv[1] == "__slot__":
    from yourharness.agent.slot import main as slot_main

    slot_main()
else:
    from yourharness.cli import main

    raise SystemExit(main())

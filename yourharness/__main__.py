import sys

if len(sys.argv) > 1 and sys.argv[1] == "__slot__":  # a worker process of a packaged build
    from .agent.slot import main as slot_main

    slot_main()
else:
    from .cli import main

    raise SystemExit(main())

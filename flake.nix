{
  description = "A durable WhatsApp archive your agent can query — with sending a human must approve";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs = { self, nixpkgs }:
    let
      lib = nixpkgs.lib;
      systems = [ "x86_64-linux" "aarch64-linux" ];
      version = "0.1.0";

      # ---------------------------------------------------------------------- #
      # 🔴 THE MODULE SPLIT, DECLARED ONCE — the only place the Nix path declares
      # it.
      #
      # The property this repo exists for: the CONSUMER parses payloads the
      # internet influences and must carry no code that can post a message, so a
      # parser bug cannot reach a send endpoint; the SENDER is the only half
      # holding a send token and carries no parser.
      #
      # `tests/test_image_split.py` guards that for the DOCKER path by reading both
      # Dockerfiles. A second build path is a second chance to lose it, so the same
      # two sets are given here and their disjointness is checked at EVAL time:
      # every `nix build` of this flake fails before compiling anything if either
      # set drifts into the other.
      # ---------------------------------------------------------------------- #
      sharedModules = [ "_wa_db.py" ];
      ingestModules = [ "consumer.py" "_wa_parse.py" "_wa_webhook.py" "_minio.py" ];
      sendModules = [ "sender.py" "_wa_outbound.py" ];

      consumerModules = ingestModules ++ sharedModules;
      senderModules = sendModules ++ sharedModules;

      splitHolds =
        lib.intersectLists consumerModules sendModules == [ ]
        && lib.intersectLists senderModules ingestModules == [ ];

      perSystem = system:
        let
          pkgs = import nixpkgs { inherit system; };
          pypkgs = pkgs.python3Packages;

          depNames = [ "psycopg2" "minio" ];
          testDepNames = depNames ++ [ "pytest" ];
          mkEnv = names:
            pkgs.python3.withPackages (ps: map (n: ps.${n}) names);

          # The image's /app, assembled from the module set — so the split above
          # is what decides the file set.
          modulesDir = name: modules: pkgs.runCommand "${name}-modules" { } ''
            mkdir -p $out/app
            ${lib.concatMapStringsSep "\n"
              (m: "cp ${self}/${m} $out/app/${m}")
              modules}
          '';

          mkImage = { name, modules, command }: pkgs.dockerTools.buildLayeredImage {
            inherit name;
            tag = version;
            contents = [
              (mkEnv depNames)
              pkgs.cacert
              (modulesDir name modules)
            ];
            extraCommands = ''
              mkdir -p tmp
              chmod 1777 tmp
            '';
            config = {
              Cmd = command;
              WorkingDir = "/app";
              User = "65532:65532";
              Env = [
                "PYTHONUNBUFFERED=1"
                "PYTHONDONTWRITEBYTECODE=1"
                # 🔴 /tmp, NOT a `$HOME` under the store. A store path is
                # read-only, so a HOME the process cannot write is a trap that only
                # fires on the first write. This pod's real state lives on mounted
                # volumes (the heartbeat file) and in Postgres.
                "HOME=/tmp"
              ];
            };
          };

          # The OPERATOR CLI — both halves, for a laptop talking to
          # WHATSAPP_PG_DSN. Shipping the union is right here and wrong in a pod,
          # which is exactly why the deployed halves are the two images below.
          app = pypkgs.buildPythonApplication {
            pname = "whatsapp-for-agents";
            inherit version;
            src = self;
            pyproject = true;
            build-system = [ pypkgs.setuptools ];
            dependencies = map (n: pypkgs.${n}) depNames;
            # 🔴 `psycopg2-binary` is the PIP distribution name; nixpkgs ships the
            # same importable module as `psycopg2`, so the runtime-dependency check
            # reports an unmet name that is in fact satisfied. Remove THAT entry
            # rather than disabling the hook (`pythonRuntimeDepsCheck = false`),
            # which would stop verifying `minio` too.
            pythonRemoveDeps = [ "psycopg2-binary" ];
            # The suite is `checks.tests`, not a build step: a packaging failure
            # and a broken test are different failures and deserve different names.
            doCheck = false;
          };
        in
        {
          packages = {
            whatsapp-for-agents = app;
            default = app;

            consumer-image = mkImage {
              name = "whatsapp-for-agents-consumer";
              modules = consumerModules;
              command = [ "${mkEnv depNames}/bin/python3" "/app/consumer.py" "serve" ];
            };
            sender-image = mkImage {
              name = "whatsapp-for-agents-sender";
              modules = senderModules;
              command = [ "${mkEnv depNames}/bin/python3" "/app/sender.py" "serve" ];
            };
          };

          checks.tests = pkgs.runCommand "whatsapp-for-agents-tests" {
            nativeBuildInputs = [ (mkEnv testDepNames) ];
          } ''
            set -o pipefail
            cp -r ${self} src
            chmod -R u+w src
            cd src
            export PYTHONDONTWRITEBYTECODE=1
            python3 -m pytest tests -q -rs -p no:cacheprovider | tee out.txt
            if grep -E '^SKIPPED' out.txt | grep -qv 'WHATSAPP_TEST_DSN'; then
              echo "a test skipped for a reason other than the absent live DSN" >&2
              grep -E '^SKIPPED' out.txt >&2
              exit 1
            fi
            touch $out
          '';

          devShells.default = pkgs.mkShell {
            packages = [ (mkEnv testDepNames) ] ++ (with pkgs; [ docker-compose ]);
            shellHook = ''
              echo "whatsapp-for-agents dev shell — python3 with psycopg2, minio, pytest"
              echo "  python3 -m pytest tests -q -rs     # hermetic; the DSN-gated modules skip"
              echo "  docker compose up -d postgres      # for the live suites, then:"
              echo "  WHATSAPP_TEST_DSN=postgresql://whatsapp_app:...@127.0.0.1:5432/whatsapp \\"
              echo "    python3 -m pytest tests -q -rs"
            '';
          };
        };

      all = lib.genAttrs systems perSystem;
    in
    if !splitHolds then
      throw ''
        the consumer/sender module split is broken: the ingest and send sets must
        stay disjoint apart from _wa_db.py. `tests/test_image_split.py` guards the
        same property for the Docker path; this is its Nix twin, and it fails at
        EVAL time so nothing builds before it is fixed.
      ''
    else
      {
        packages = lib.mapAttrs (_: v: v.packages) all;
        checks = lib.mapAttrs (_: v: v.checks) all;
        devShells = lib.mapAttrs (_: v: v.devShells) all;
      };
}

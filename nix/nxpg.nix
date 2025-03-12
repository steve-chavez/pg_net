{ stdenv, lib, makeWrapper, fetchurl, writeShellScriptBin, findutils, entr, callPackage, lcov, pidFileName, gnused, python3, gdb, writeText} :
let
  prefix = "nxpg";
  ourPg = callPackage ./postgresql {
    inherit lib;
    inherit stdenv;
    inherit fetchurl;
    inherit makeWrapper;
    inherit callPackage;
  };
  supportedPgs = [
    ourPg.postgresql_17
    ourPg.postgresql_16
    ourPg.postgresql_15
    ourPg.postgresql_14
    ourPg.postgresql_13
    ourPg.postgresql_12
  ];
  blo =
    let
      gdbConf = writeText "gdbconf" ''
        # Do this so we can do `backtrace` once a segfault occurs. Otherwise once SIGSEGV is received the bgworker will quit and we can't backtrace.
        handle SIGSEGV stop nopass
      '';
    in
    ((callPackage ./checked-shell-script.nix {})
    {
      name = "nxpg-v2";
      docs = "Build a postgres extension";
      args = [
        "ARG_POSITIONAL_SINGLE([operation], [Operation])"
        "ARG_TYPE_GROUP_SET([OPERATION], [OPERATION], [operation], [build,test,coverage,psql,gdb])"
        "ARG_OPTIONAL_SINGLE([version], [v], [PostgreSQL version], [17])"
        "ARG_TYPE_GROUP_SET([VERSION], [VERSION], [version], [17,16,15,14,13,12])"
        "ARG_LEFTOVERS([psql arguments])"
      ];
    }
    ''
    case "$_arg_version" in
      17)
        export PATH=${ourPg.postgresql_17}/bin:"$PATH"
        ;;
      16)
        export PATH=${ourPg.postgresql_16}/bin:"$PATH"
        ;;
      15)
        export PATH=${ourPg.postgresql_15}/bin:"$PATH"
        ;;
      14)
        export PATH=${ourPg.postgresql_14}/bin:"$PATH"
        ;;
      13)
        export PATH=${ourPg.postgresql_13}/bin:"$PATH"
        ;;
      12)
        export PATH=${ourPg.postgresql_12}/bin:"$PATH"
        ;;
    esac

    export BUILD_DIR="build-$_arg_version"

    # fail fast for gdb command requirement
    pid_file_name="$BUILD_DIR"/bgworker.pid

    if [ "$_arg_operation" == gdb ] && [ ! -e "$pid_file_name" ]; then
        echo 'The background worker is not started. First you have to run "nxpg psql".'
        exit 1
    fi

    # commands that require the build ready
    case "$_arg_operation" in
      coverage)
        make COVERAGE=1
        ;;

      gdb)
        # not required here, do nothing
        ;;

      *)
        make
        ;;
    esac

    # commands that require a temp db
    if [ "$_arg_operation" != build ] && [ "$_arg_operation" != gdb ]; then
      tmpdir="$(mktemp -d)"

      export PGDATA="$tmpdir"
      export PGHOST="$tmpdir"
      export PGUSER=postgres
      export PGDATABASE=postgres

      trap 'pg_ctl stop -m i && rm -rf "$tmpdir" && rm "$pid_file_name"' sigint sigterm exit

      PGTZ=UTC initdb --no-locale --encoding=UTF8 --nosync -U "$PGUSER"

      # pg versions older than 16 don't support adding "-c" to initdb to add these options
      # so we just modify the resulting postgresql.conf to avoid an error
      echo "dynamic_library_path='\$libdir:$(pwd)/$BUILD_DIR'" >> "$PGDATA"/postgresql.conf
      echo "extension_control_path='\$system:$(pwd)/$BUILD_DIR'" >> "$PGDATA"/postgresql.conf

      options="-F -c listen_addresses=\"\" -c log_min_messages=\"''${LOG_MIN_MESSAGES:-INFO}\" -k $PGDATA"

      ext_options="-c shared_preload_libraries=pg_net"

      pg_ctl start -o "$options" -o "$ext_options"

      # save pid for future gdb invocation
      psql -t -c "\o $pid_file_name" -c "select pid from pg_stat_activity where backend_type ilike '%pg_net%'"
      ${gnused}/bin/sed '/^''$/d;s/[[:blank:]]//g' -i "$pid_file_name"
    fi

    case "$_arg_operation" in
      build)
        # do nothing here as the build already ran
        ;;

      test)
        ${python3}/bin/python -m pytest -vv "''${_arg_leftovers[@]}"
        ;;

      coverage)
        info_file="coverage.info"
        out_dir="coverage_html"

        ${python3}/bin/python -m pytest -vv

        ${lcov}/bin/lcov --capture --directory . --output-file "$info_file"

        # remove postgres headers on the nix store, otherwise they show on the output
        ${lcov}/bin/lcov --remove "$info_file" '/nix/*' --output-file "$info_file" || true

        ${lcov}/bin/lcov --list coverage.info
        ${lcov}/bin/genhtml "$info_file" --output-directory "$out_dir"

        echo -e "\nTo see the results, visit file://$(pwd)/$out_dir/index.html on your browser\n"
        ;;

      psql)
        psql "''${_arg_leftovers[@]}"
        ;;

      gdb)

        if [ "$EUID" != 0 ]; then
          echo 'Prefix the command with "sudo", gdb requires elevated privileges to debug processes.'
          exit 1
        fi

        pid=$(cat "$pid_file_name")
        ${gdb}/bin/gdb -x ${gdbConf} -p "$pid" "''${_arg_leftovers[@]}"
        ;;

      esac
    '').bin;
  build =
    writeShellScriptBin "${prefix}-build" ''
      set -euo pipefail

      make clean
      make
    '';
  buildCov =
    writeShellScriptBin "${prefix}-build-cov" ''
      set -euo pipefail

      make clean
      make COVERAGE=1
    '';
  test =
    writeShellScriptBin "${prefix}-test" ''
      set -euo pipefail

      ${python3}/bin/python -m pytest -vv "$@"
    '';
  cov =
    writeShellScriptBin "${prefix}-coverage" ''
      set -euo pipefail

      info_file="coverage.info"
      out_dir="coverage_html"

      ${python3}/bin/python -m pytest -vv "$@"

      ${lcov}/bin/lcov --capture --directory . --output-file "$info_file"

      # remove postgres headers on the nix store, otherwise they show on the output
      ${lcov}/bin/lcov --remove "$info_file" '/nix/*' --output-file "$info_file" || true

      ${lcov}/bin/lcov --list coverage.info
      ${lcov}/bin/genhtml "$info_file" --output-directory "$out_dir"

      echo "${prefix}-coverage: To see the results, visit file://$(pwd)/$out_dir/index.html on your browser"
    '';
  watch =
    writeShellScriptBin "${prefix}-watch" ''
      set -euo pipefail

      ${findutils}/bin/find . -type f \( -name '*.c' -o -name '*.h' \) | ${entr}/bin/entr -dr "$@"
    '';

  tmpDb =
    writeShellScriptBin "${prefix}-tmp" ''
      set -euo pipefail

      export tmpdir="$(mktemp -d)"

      export PGDATA="$tmpdir"
      export PGHOST="$tmpdir"
      export PGUSER=postgres
      export PGDATABASE=postgres

      trap 'pg_ctl stop -m i && rm -rf "$tmpdir" && rm ${pidFileName}' sigint sigterm exit

      PGTZ=UTC initdb --no-locale --encoding=UTF8 --nosync -U "$PGUSER"

      # pg versions older than 16 don't support adding "-c" to initdb to add these options
      # so we just modify the resulting postgresql.conf to avoid an error
      echo "dynamic_library_path='\$libdir:$(pwd)'" >> $PGDATA/postgresql.conf
      echo "extension_control_path='\$system:$(pwd)'" >> $PGDATA/postgresql.conf

      options="-F -c listen_addresses=\"\" -c log_min_messages=\"''${LOG_MIN_MESSAGES:-INFO}\" -k $PGDATA"

      ext_options="-c shared_preload_libraries=\"pg_net\""

      pg_ctl start -o "$options" -o "$ext_options"

      psql -v ON_ERROR_STOP=1 -c "create database pre_existing" -d postgres

      psql -v ON_ERROR_STOP=1 -c "create role pre_existing nosuperuser login" -d postgres

      psql -v ON_ERROR_STOP=1 -c "create extension pg_net" -d postgres

      psql -t -c "\o ${pidFileName}" -c "select pid from pg_stat_activity where backend_type ilike '%pg_net%'"

      ${gnused}/bin/sed '/^''$/d;s/[[:blank:]]//g' -i ${pidFileName}

      psql -f ${./bench.sql}

      "$@"
    '';
    allPgPaths = map (pg:
      let
        ver = builtins.head (builtins.splitVersion pg.version);
        script = ''
          set -euo pipefail

          export PATH=${pg}/bin:"$PATH"

          "$@"
        '';
      in
      writeShellScriptBin "${prefix}-${ver}" script
    ) supportedPgs;

    netWith = map (pg:
        let
          ver = builtins.head (builtins.splitVersion pg.version);
          script = ''
            set -euo pipefail
            export PATH=${pg}/bin:"$PATH"
            ${buildCov}/bin/${prefix}-build-cov
            ${tmpDb}/bin/${prefix}-tmp "$@"
          '';
        in
        writeShellScriptBin "net-with-pg-${ver}" script
    ) supportedPgs;
in
[
  build
  buildCov
  test
  cov
  watch
  tmpDb
  allPgPaths
  netWith
  blo
]

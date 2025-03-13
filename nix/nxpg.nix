{ stdenv, lib, makeWrapper, fetchurl, writeShellScriptBin, findutils, entr, callPackage, lcov, gnused, gdb, writeText} :
let
  ourPg = callPackage ./postgresql {
    inherit lib;
    inherit stdenv;
    inherit fetchurl;
    inherit makeWrapper;
    inherit callPackage;
  };
  blo =
    let
      makefileContents = builtins.readFile ../Makefile;

      # Replace possible Windows carriage returns and split into lines
      lines = lib.splitString "\n"
        (builtins.replaceStrings [ "\r" ] [ "" ] makefileContents);

      # Filter lines that match regex
      matchingLines = builtins.filter (line: builtins.match "^EXTENSION *= *(.*)$" line != null) lines;

      # If we found any matches, extract from the first; else "UNKNOWN"
      extensionName =
        if builtins.length matchingLines == 0 then
          "UNKNOWN"
        else
          let
            tokens = builtins.match "^EXTENSION *= *(.*)$" (builtins.head matchingLines);
          in
            builtins.elemAt tokens 0;

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

      ext_options="-c shared_preload_libraries=${extensionName}"

      pg_ctl start -o "$options" -o "$ext_options"

      createdb contrib_regression

      init_file=test/init.sql

      if [ -f $init_file ]; then
        psql -v ON_ERROR_STOP=1 -f $init_file -d contrib_regression
      fi

      # save pid for future gdb invocation
      psql -t -c "\o $pid_file_name" -c "select pid from pg_stat_activity where backend_type ilike '%${extensionName}%'"
      ${gnused}/bin/sed '/^''$/d;s/[[:blank:]]//g' -i "$pid_file_name"
    fi

    case "$_arg_operation" in
      build)
        # do nothing here as the build already ran
        ;;

      test)
        make test
        ;;

      coverage)
        info_file="coverage.info"
        out_dir="coverage_html"

        make test

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
        if [ -z "$pid" ]; then
          echo "There's no background worker found for extension ${extensionName}"
          exit 1
        fi
        ${gdb}/bin/gdb -x ${gdbConf} -p "$pid" "''${_arg_leftovers[@]}"
        ;;

      esac
    '').bin;
in
[
  blo
]

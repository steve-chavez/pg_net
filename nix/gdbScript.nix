{ gdb, writeText, writeShellScriptBin } :

let
  file = writeText "gdbconf" ''
    # Do this so we can backtrace, otherwise once SIGSEGV is received we can't as the bgworker will quit
    handle SIGSEGV stop nopass
  '';
  script = ''
    ${gdb}/bin/gdb -x ${file} "$@"
  '';
in
writeShellScriptBin "with-gdb" script

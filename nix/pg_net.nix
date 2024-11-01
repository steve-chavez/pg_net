{ stdenv, postgresql, curl, lib, libkqueue, pkg-config }:

stdenv.mkDerivation {
  name = "pg_net";

  buildInputs =
    [ postgresql curl pkg-config ] ++
    lib.optional stdenv.isLinux [libkqueue];

  src = ../.;

  # this is enough for enabling debug info
  dontStrip = true;

  installPhase = ''
    mkdir -p $out/bin
    install -D *.{dylib,so} -t $out/lib

    install -D -t $out/share/postgresql/extension sql/*.sql
    install -D -t $out/share/postgresql/extension pg_net.control
  '';
}

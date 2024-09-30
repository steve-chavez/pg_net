with import (builtins.fetchTarball {
    name = "24.05"; # May 31 2024
    url = "https://github.com/NixOS/nixpkgs/archive/refs/tags/24.05.tar.gz";
    sha256 = "sha256:1lr1h35prqkd1mkmzriwlpvxcb34kmhc9dnr48gkm8hh089hifmx";
}) {};
mkShell {
  buildInputs =
    let
      ourCares = callPackage ./nix/c-ares.nix {};
      ourCurl = callPackage ./nix/curl.nix { c-aresMinimal = ourCares;};
      pidFileName = "net_worker.pid";
      supportedPgVersions = [
        postgresql_12
        postgresql_13
        postgresql_14
        postgresql_15
        postgresql_16
      ];
      pgWithExt = { pg }: pg.withPackages (p: [ (callPackage ./nix/pg_net.nix { postgresql = pg; curl = ourCurl; }) ]);
      extAll = map (x: callPackage ./nix/pgScript.nix { postgresql = pgWithExt { pg = x;}; inherit pidFileName;}) supportedPgVersions;
      gdbScript = callPackage ./nix/gdbScript.nix {inherit pidFileName;};
      nginxCustom = callPackage ./nix/nginxCustom.nix {};
      nixopsScripts = callPackage ./nix/nixopsScripts.nix {};
      pythonDeps = with python3Packages; [
        pytest
        psycopg2
        sqlalchemy
      ];
      format = callPackage ./nix/format.nix {};
    in
    [
      extAll
      pythonDeps
      format.do format.doCheck
      nginxCustom.nginxScript
      gdbScript
      bcc
      ourCares
    ] ++ nixopsScripts;
  shellHook = ''
    export HISTFILE=.history
  '';
}

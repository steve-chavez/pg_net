{ fetchFromGitHub } :
let
  nxpg = fetchFromGitHub {
    owner  = "steve-chavez";
    repo   = "nxpg";
    rev    = "v1.0";
    sha256 = "sha256-OjKARi8MIqtn76JZEnyM7F9GsRuySc2uPLUwXGcHAw0=";
  };
  script = import nxpg;
in
script

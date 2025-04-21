{ pkgs ? import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/26d499fc9f1d567283d5d56fcf367edd815dba1d.tar.gz") {} }:

let
  golangci-lint = pkgs.golangci-lint.overrideAttrs (oldAttrs: rec {
    version = "1.59.1";
    src = pkgs.fetchFromGitHub {
      owner = "golangci";
      repo = "golangci-lint";
      rev = "v${version}";
      hash = "sha256-VFU/qGyKBMYr0wtHXyaMjS5fXKAHWe99wDZuSyH8opg";
    };
    vendorHash = "sha256-yYwYISK1wM/mSlAcDSIwYRo8sRWgw2u+SsvgjH+Z/7M";
    ldflags = [
      "-s"
      "-X main.version=${version}"
      "-X main.commit=v${version}"
      "-X main.date=19700101-00:00:00"
    ];
  });
in
pkgs.mkShell {
  buildInputs = [
    golangci-lint
    pkgs.git
    pkgs.go_1_22
    pkgs.gotools
    pkgs.pigeon
  ];
}

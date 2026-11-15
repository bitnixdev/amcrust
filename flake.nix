{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    # various, usually obscure, programs that are missing from nixpkgs
    nixpkgs-staging.url = "github:jasonrm/nixpkgs-staging";

    chips = {
      url = "github:jasonrm/nix-chips";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.nixpkgs-staging.follows = "nixpkgs-staging";
    };
  };

  outputs = inputs @ {chips, nixpkgs, ...}: let
    base = chips.lib.mkFlake {
      inherit inputs;
      sources.packages = ./nix/packages;
      sources.nixosModules = ./nix/nixosModules;
    };
  in base // {
    # This application needs a library/toolchain shell, not chips' NixOS service
    # shims (which import modules removed from the pinned nixpkgs).
    devShells = nixpkgs.lib.genAttrs (builtins.attrNames base.packages) (system: {
      default = import ./nix/devShells/default.nix {pkgs = nixpkgs.legacyPackages.${system};};
    });
  };
}

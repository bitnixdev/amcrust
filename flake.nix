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

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    chips,
    rust-overlay,
    ...
  }:
    chips.lib.use {
      # Generate new devShells with `nix run .#init-dev-shell <GITHUB_USERNAME>`
      devShellsDir = ./nix/devShells;
      packagesDir = ./nix/packages;
      # nixosModulesDir = ./nix/nixosModules;
      overlays = [
        rust-overlay.overlays.default
      ];
    };
}

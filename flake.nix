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

  outputs = inputs @ {chips, ...}:
    chips.lib.mkFlake {
      inherit inputs;
      # Generate new devShells with `nix run .#init-dev-shell <GITHUB_USERNAME>`
      sources.devShells = ./nix/devShells;
      sources.packages = ./nix/packages;
      sources.nixosModules = ./nix/nixosModules;
    };
}

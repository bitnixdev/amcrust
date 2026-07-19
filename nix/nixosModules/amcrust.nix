{
  config,
  lib,
  pkgs,
  ...
}: let
  inherit (lib) mkEnableOption mkIf mkOption types;

  cfg = config.services.amcrust;

  cameraModule = {name, ...}: {
    options = {
      name = mkOption {
        type = types.str;
        default = name;
        description = "HomeKit accessory name.";
      };

      host = mkOption {
        type = types.str;
        description = "IP address or hostname of the Amcrest camera.";
      };

      username = mkOption {
        type = types.str;
        default = "admin";
        description = "Camera API username.";
      };

      passwordFile = mkOption {
        type = types.str;
        description = ''
          Path to a file containing only the camera API password. The file is
          loaded with systemd credentials and is not copied to the Nix store.
        '';
      };

      hapPort = mkOption {
        type = types.nullOr types.port;
        default = null;
        description = ''
          HomeKit Accessory Protocol TCP port. When unset, amcrust chooses and
          persists a free port.
        '';
      };

      pin = mkOption {
        type = types.nullOr types.str;
        default = null;
        description = ''
          HomeKit setup PIN. Leave unset to generate and persist a random PIN.
          This value is written to the Nix store when set.
        '';
      };

      rtspSubtype = mkOption {
        type = types.ints.between 0 2;
        default = 2;
        description = "RTSP stream subtype used for HomeKit live video.";
      };

      audio = mkOption {
        type = types.bool;
        default = true;
        description = "Whether to transcode and send audio in live view.";
      };

      metricsPort = mkOption {
        type = types.port;
        default = 0;
        description = ''
          TCP port for the health and Prometheus metrics server. Zero lets the
          operating system choose a port.
        '';
      };

      saveSnapshots = mkOption {
        type = types.bool;
        default = false;
        description = "Whether to save the most recently served snapshot.";
      };

      extraArgs = mkOption {
        type = types.listOf types.str;
        default = [];
        description = "Additional command-line arguments passed to amcrust.";
      };
    };
  };

  mkCameraService = camera: {
    description = "Amcrust camera capture for ${camera.name}";
    wantedBy = ["multi-user.target"];
    after = ["network-online.target"];
    wants = ["network-online.target"];

    environment =
      {
        CAMERA_NAME = camera.name;
        CAMERA_HOST = camera.host;
        AMCREST_USERNAME = camera.username;
        DATA_DIR = cfg.dataDir;
        RTSP_SUBTYPE = toString camera.rtspSubtype;
        AUDIO = lib.boolToString camera.audio;
        METRICS_PORT = toString camera.metricsPort;
        SAVE_SNAPSHOTS = lib.boolToString camera.saveSnapshots;
      }
      // lib.optionalAttrs (camera.hapPort != null) {
        HAP_PORT = toString camera.hapPort;
      }
      // lib.optionalAttrs (camera.pin != null) {
        HAP_PIN = camera.pin;
      };

    path = [pkgs.ffmpeg];
    script = ''
      export AMCREST_PASSWORD="$(<"$CREDENTIALS_DIRECTORY/password")"
      exec ${lib.getExe cfg.package} ${lib.escapeShellArgs camera.extraArgs}
    '';

    serviceConfig = {
      User = cfg.user;
      Group = cfg.group;
      WorkingDirectory = cfg.dataDir;
      LoadCredential = "password:${camera.passwordFile}";
      Restart = "on-failure";
      RestartSec = 5;

      NoNewPrivileges = true;
      PrivateTmp = true;
      ProtectHome = true;
      ProtectSystem = "strict";
      ReadWritePaths = [cfg.dataDir];
    };
  };
in {
  options.services.amcrust = {
    enable = mkEnableOption "Amcrust camera capture";

    package = mkOption {
      type = types.package;
      default = pkgs.amcrust;
      defaultText = lib.literalExpression "pkgs.amcrust";
      description = "The amcrust package to run.";
    };

    dataDir = mkOption {
      type = types.str;
      default = "/var/lib/amcrust";
      description = "Directory containing persistent HomeKit pairing state.";
    };

    user = mkOption {
      type = types.str;
      default = "amcrust";
      description = "User under which the camera services run.";
    };

    group = mkOption {
      type = types.str;
      default = "amcrust";
      description = "Group under which the camera services run.";
    };

    cameras = mkOption {
      type = types.attrsOf (types.submodule cameraModule);
      default = {};
      example = lib.literalExpression ''
        {
          frontyard = {
            host = "192.168.1.50";
            passwordFile = "/run/secrets/amcrest-frontyard";
            hapPort = 51826;
            metricsPort = 9090;
          };
        }
      '';
      description = "Amcrest cameras to expose as independent HomeKit accessories.";
    };
  };

  config = mkIf cfg.enable {
    assertions = [
      {
        assertion = cfg.cameras != {};
        message = "services.amcrust.cameras must define at least one camera";
      }
    ];

    users.groups = mkIf (cfg.group == "amcrust") {
      amcrust = {};
    };

    users.users = mkIf (cfg.user == "amcrust") {
      amcrust = {
        isSystemUser = true;
        group = cfg.group;
        description = "Amcrust camera capture service";
      };
    };

    systemd.tmpfiles.rules = [
      "d '${cfg.dataDir}' 0750 ${cfg.user} ${cfg.group} - -"
    ];

    systemd.services =
      lib.mapAttrs' (
        instance: camera:
          lib.nameValuePair "amcrust-${instance}" (mkCameraService camera)
      )
      cfg.cameras;
  };
}

#!/bin/bash
CLASSPATH_PREFIX=`yarn classpath` ./app-launcher.sh --app_spec_provider org.apache.helix.provisioning.yarn.example.MyTaskAppSpecFactory --app_config_spec /home/me/job_runner_app_spec.yaml

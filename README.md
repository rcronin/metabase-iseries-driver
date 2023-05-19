# How to build

## ENV needed - full path of this repo
DRIVER_PATH

## Command - run in local mapped metabase repo
clojure \
  -Sdeps "{:aliases {:iseries {:extra-deps {com.metabase/iseries-driver {:local/root \"$DRIVER_PATH\"}}}}}"  \
  -X:build:iseries \
  build-drivers.build-driver/build-driver! \
  "{:driver :iseries, :project-dir \"$DRIVER_PATH\", :target-dir \"$DRIVER_PATH/target\"}"
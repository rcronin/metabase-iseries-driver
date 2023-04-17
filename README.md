# How to build

## ENV needed - full path of this repo
DRIVER_PATH

## Command - run in local mapped metabase repo
clojure \
  -Sdeps "{:aliases {:db2 {:extra-deps {com.metabase/db2-driver {:local/root \"$DRIVER_PATH\"}}}}}"  \
  -X:build:db2 \
  build-drivers.build-driver/build-driver! \
  "{:driver :db2, :project-dir \"$DRIVER_PATH\", :target-dir \"$DRIVER_PATH/target\"}"
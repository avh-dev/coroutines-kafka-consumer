rootProject.name = "coroutines-kafka-consumer"

include(":ckc-core")
include(":ckc-micrometer")
include(":ckc-experiments")
include(":ckc-demo-contracts")
include(":ckc-demo-audit")
include(":ckc-demo")
include(":ckc-demo-load-test")
include(":ckc-demo-stubs")

project(":ckc-experiments").projectDir = file("experiments/ckc-experiments")
project(":ckc-demo-contracts").projectDir = file("demo/ckc-demo-contracts")
project(":ckc-demo-audit").projectDir = file("demo/ckc-demo-audit")
project(":ckc-demo").projectDir = file("demo/ckc-demo")
project(":ckc-demo-load-test").projectDir = file("demo/ckc-demo-load-test")
project(":ckc-demo-stubs").projectDir = file("demo/ckc-demo-stubs")

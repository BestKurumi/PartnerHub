# 开发与验证

默认入口与两种运行方式见根README。Go版本以go.mod和Dockerfile为准，Python脚本仅用标准库。

## 常规检查

```sh
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go build ./cmd/...
python3 -m unittest discover -s scripts -p demo_test.py
```

浏览器回归需要Node及Playwright。可把依赖安装在被gitignore忽略的本地目录：

```sh
npm install --prefix .local/browser playwright
npx --prefix .local/browser playwright install chromium
# macOS/Linux
NODE_PATH="$PWD/.local/browser/node_modules" node --test web/console_browser_test.js
# PowerShell
$env:NODE_PATH="$PWD/.local/browser/node_modules"
node --test web/console_browser_test.js
```

默认使用Playwright管理的Chromium；已有Chrome时通过`CHROME_PATH`指定路径。`bash scripts/check-console-local.sh`汇总Go、Node、浏览器和Compose静态检查，适用于有Bash的开发环境。单独运行各条命令可用于PowerShell。

## 集成测试

`deploy/test/compose.yml`是独立测试依赖，使用13306／16379／19092和固定的本机测试凭据，与默认Demo分开：

```sh
make test-deps
```

没有Make时按该目标中的四条Docker Compose命令执行。按测试所需配置`HUB_TEST_DSN`、`PLATFORM1_TEST_DSN`、`PLATFORM2_TEST_DSN`、`REDIS_TEST_ADDR`、`KAFKA_TEST_BROKERS`后运行：

```sh
go test -tags=integration ./tests/integration -count=1
```

带`e2e`标签的历史故障矩阵部分用例有作者隔离项目授权门禁，普通clone不应直接运行整套E2E或绕过门禁；`go test ./...`不会执行这些有标签用例。可从README的页面操作完成正常端到端验收。负载工具`tests/load`需单独配置目标和数据库，不把一次本机结果解释为生产容量。

## 备用镜像构建

正常`python3 scripts/demo.py up`在Docker内编译，不需要宿主Go。若构建镜像下载受限，但本机Go和中间件镜像已齐备，可使用：

```sh
make build-local-image
docker compose --env-file .env up -d --no-build --wait --wait-timeout 300
```

备用路径按Docker服务端amd64／arm64选择Linux架构，可显式设置`HUB_DOCKER_ARCH`。本地运行层仅打包二进制与页面、迁移，不包含Go工具链。该路径不等于整套离线安装；普通使用者优先选择Docker默认runtime目标。

提交中保留产品源码、测试、迁移、可复现配置及已归档的OpenSpec变更文档与配图；忽略`.env*`（示例除外）、`.local/`、编译产物、浏览器依赖、AI工作流状态和原始执行报告。归档内的`.comet/`与`.comet.yaml`继续忽略，已有本地资料不会被删除。归档状态和历史引用说明见[OpenSpec索引](openspec/README.md)。

## 本地模式的高级配置

通常只需编辑 `.env.native` 的中间件地址和密码，再运行 `python3 scripts/demo.py native-up`。Go工具链会自动管理go.mod依赖；入口脚本只负责加载配置、初始化和管理本次子进程。

需要单独初始化时可执行 `python3 scripts/demo.py native-migrate`。它创建缺失的两个Kafka Topic和三个MySQL schema并迁移；不重置数据。`native-up`已包含此步骤。

可选覆盖 `HUB_LISTEN`、`NATIVE_PLATFORM1_LISTEN`、`NATIVE_PLATFORM2_LISTEN`、`NATIVE_UPSTREAM_LISTEN`，默认分别为127.0.0.1的28180／28081／28082／28083；对应代理地址和中台发送地址自动派生。`NATIVE_GATEWAY_LISTEN`默认127.0.0.1:28080。应用均在本机运行，网关只接受loopback后端。


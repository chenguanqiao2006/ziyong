name: 月度全量刷新

on:
  schedule:
    # 每月1日 UTC 10:00 = 北京时间 18:00
    # 依据1：baostock 日K数据当日 17:30 完成入库，18:00 拉取必含最新交易日
    # 依据2：每日增量长期在 17:45 稳定运行，该时段服务可用性已被反复验证
    # 依据3：与 daily 共用 concurrency 组，即便重叠也只是排队，不会互相覆盖
    # ⚠️ 勿改到北京时间 23:00~08:00：baostock 夜间时段不可用（2026-10-09 01:39 实测三连败）
    - cron: '0 10 1 * *'
  workflow_dispatch:        # 手动触发（断点续跑/补跑用）

permissions:
  contents: write           # 脚本内checkpoint提交需要写权限

concurrency:
  group: ashare-data-pipeline   # ★ 与daily共用group，绝不并行
  cancel-in-progress: false

jobs:
  refresh:
    runs-on: ubuntu-latest
    timeout-minutes: 300    # 5小时硬上限（Actions单job上限6小时）
    steps:
      - name: 检出仓库
        uses: actions/checkout@v4

      - name: 配置git身份（脚本内checkpoint提交要用）
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

      - name: 安装Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: 'pip'

      - name: 安装依赖（版本锁定）
        run: pip install -r requirements.txt

      - name: 运行月度刷新
        env:
          PYTHONUNBUFFERED: '1'   # 2~3小时长任务，日志实时显示
        run: python src/monthly_refresh.py

      - name: 收尾提交推送
        run: |
          git add data
          git commit -m "🔄 月度全量刷新 $(TZ=Asia/Shanghai date +%F)" || echo "无变更"
          git pull --rebase
          git push

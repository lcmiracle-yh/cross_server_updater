开发环境|Dev Environment：Java SDK 12.

使用说明|Instructions：Build后，在\out\artifacts\db_sync_jar下运行db-sync.jar。

设置|config：

	warning_level：值0-2

	source：源数据库uri

	target：目标数据库uri

	start_time：首次运行更新的时间。24小时制。

	update_interval：更新间段时长（秒）。e.g.如每天更新一次，则使用86400秒（24小时）。

	debug_quick_start：测试用。立刻更新。

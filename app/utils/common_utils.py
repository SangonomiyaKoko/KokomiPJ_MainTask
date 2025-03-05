from .time_utils import TimeFormat


class CommonUtils:
    def get_active_level(is_public, total_battles, last_battle_time):
        "获取active_level"
        # 具体对应关系的表格
        # | is_plblic | total_battles | last_battle_time | active_level | decs    |
        # | --------- | ------------- | ---------------- | ------------ | ------- |
        # | 0         | -             | -                | 0            | 隐藏战绩 |
        # | 1         | 0             | 0                | 1            | 无数据   |
        # | 1         | -             | [0, 1d]          | 2            | 活跃    |
        # | 1         | -             | [1d, 3d]         | 3            | -       |
        # | 1         | -             | [3d, 7d]         | 4            | -       |
        # | 1         | -             | [7d, 1m]         | 5            | -       |
        # | 1         | -             | [1m, 3m]         | 6            | -       |
        # | 1         | -             | [3m, 6m]         | 7            | -       |
        # | 1         | -             | [6m, 1y]         | 8            | -       |
        # | 1         | -             | [1y, + ]         | 9            | 不活跃  |
        if not is_public:
            return 0
        if total_battles == 0 or last_battle_time == 0:
            return 1
        current_timestamp = TimeFormat.get_current_timestamp()
        time_differences = [
            (1 * 24 * 60 * 60, 2),
            (3 * 24 * 60 * 60, 3),
            (7 * 24 * 60 * 60, 4),
            (30 * 24 * 60 * 60, 5),
            (90 * 24 * 60 * 60, 6),
            (180 * 24 * 60 * 60, 7),
            (360 * 24 * 60 * 60, 8),
        ]
        time_since_last_battle = current_timestamp - last_battle_time
        for time_limit, return_value in time_differences:
            if time_since_last_battle <= time_limit:
                return return_value
        return 9
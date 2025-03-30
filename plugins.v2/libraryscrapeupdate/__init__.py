from datetime import datetime, timedelta
from threading import Event
from typing import Generator, List, Tuple, Dict, Any, Optional
import time

import pytz
from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore
from apscheduler.triggers.cron import CronTrigger  # type: ignore

from app.api.endpoints.media import scrape
from app.core.config import settings
from app.chain.storage import StorageChain
from app.schemas import FileItem, NotificationType
from app.schemas.types import StorageSchema
from app.db.transferhistory_oper import TransferHistoryOper
from app.log import logger
from app.plugins import _PluginBase


class LibraryScrapeUpdate(_PluginBase):
    # 插件名称
    plugin_name = "媒体库刮削更新"
    # 插件描述
    plugin_desc = "更新刮削近期已成功入库电影/电视剧，补全缺失信息。"
    # 插件图标
    plugin_icon = "scraper.png"
    # 插件版本
    plugin_version = "1.0"
    # 插件作者
    plugin_author = "Akimio521"
    # 作者主页
    author_url = "https://github.com/Akimio521"
    # 插件配置项ID前缀
    plugin_config_prefix = "libraryscrapeupdate_"
    # 加载顺序
    plugin_order = 7
    # 可使用的用户级别
    user_level = 1

    # 私有属性
    transferhis = TransferHistoryOper()
    storagechain = StorageChain()
    _scheduler: BackgroundScheduler | None = None

    _enabled: bool = False  # 运行状态
    _onlyonce: bool = False  # 立即运行
    _notify: bool = False  # 通知推送
    _detail_notify: bool = False  # 详细通知推送
    _days: int = 7  # 更新刮削几天内入库的文件
    _cron: str = "0 0 */7 * *"  # 执行周期
    _target_type: str = StorageSchema.Local.value  # 媒体库类型
    _target_path: str = ""  # 媒体库路径

    _event = Event()  # 退出事件

    def init_plugin(self, config: Optional[Dict[str, Any]] = None) -> None:
        # 读取配置
        if config:
            self._enabled = config.get("enabled") or False
            self._onlyonce = config.get("onlyonce") or False
            self._notify = config.get("notify") or False
            self._detail_notify = config.get("detail_notify") or False
            self._days = int(config.get("days") or 7)
            self._cron = config.get("cron") or "0 0 */7 * *"
            self._target_type = config.get("target_type") or StorageSchema.Local.value
            self._target_path = config.get("target_path") or ""
        logger.info(f"插件配置：{self.config}")

        self.stop_service()  # 停止现有任务

        # 立即运行一次
        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.__update_library_scrape,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ))
                + timedelta(seconds=3),
                name="媒体库刮削更新",
            )
            self._onlyonce = False  # 关闭一次性开关
            self.update_config(self.config)
            if self._scheduler.get_jobs():
                # 启动服务
                self._scheduler.print_jobs()
                self._scheduler.start()

    @property
    def config(self) -> Dict[str, Any]:
        return {
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "notify": self._notify,
            "detail_notify": self._detail_notify,
            "days": self._days,
            "cron": self._cron,
            "target_type": self._target_type,
            "target_path": self._target_path,
        }

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册服务
        """
        if self._enabled:
            logger.info("插件已启用，注册服务")
            return [
                {
                    "id": "LibraryScrapeUpdate",
                    "name": "媒体库刮削更新",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.__update_library_scrape,
                    "kwargs": {"cron_trigger": True},
                }
            ]
        else:
            logger.warning("插件未启用，取消服务")
            return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "onlyonce",
                                            "label": "立即运行",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify",
                                            "label": "通知推送",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "detail_notify",
                                            "label": "详细通知推送",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "days",
                                            "label": "更新刮削几天内入库的文件",
                                            "rows": 1,
                                            "placeholder": "默认7天",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VCronField",
                                        "props": {
                                            "model": "cron",
                                            "label": "执行周期",
                                            "placeholder": "5位cron表达式，默认'0 0 */7 * *'",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "target_type",
                                            "label": "媒体库类型",
                                            "items": [
                                                {"title": s.name, "value": s.value}
                                                for s in StorageSchema
                                            ],
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 9},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "target_path",
                                            "label": "媒体库路径",
                                            "rows": 1,
                                            "placeholder": "更新刮削媒体目录路径",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "notify": False,
            "detail_notify": False,
            "days": 7,
            "cron": "0 0 */7 * *",
            "target_type": StorageSchema.Local.value,
            "target_path": "",
        }

    def get_page(self) -> List[dict]:
        pass

    def __update_library_scrape(self, cron_trigger: bool = False) -> None:
        """
        开始更新媒体库刮削
        """
        date = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(time.time() - 86400 * self._days)
        )
        start_time = datetime.now(tz=pytz.timezone(settings.TZ))
        logger.info(
            f"开始更新刮削【{StorageSchema(self._target_type).name}】{self._target_path} 媒体在 {date} 之后入库的文件"
        )
        if self._notify:
            self.post_message(
                mtype=NotificationType.Plugin,
                title="【插件】媒体刮削更新开始运行",
                text=f"媒体库：【{StorageSchema(self._target_type).name}】{self._target_path}\n更新入库时间晚于 {date} 的文件\n触发方式："
                + f"定时任务 {self._cron}"
                if cron_trigger
                else "手动触发",
            )
        msgs: List[str] = []
        for file, history_date in self.__list_files(
            self._target_type, self._target_path
        ):
            if self._event.is_set():
                logger.warning("媒体库刮削更新服务已停止！")
                return

            logger.debug(f"文件信息：{file}")
            scrape(file, self._target_type)
            msg = f"{file.name}（{history_date}）"
            msgs.append(msg)
            logger.info(msg + "：更新刮削完成")

        waste_time = datetime.now(tz=pytz.timezone(settings.TZ)) - start_time
        logger.info(
            f"更新 【{StorageSchema(self._target_type).name}】{self._target_path} 媒体库刮削完成，耗时：{waste_time}，更新任务数：{len(msgs)}"
        )
        if self._notify:
            self.post_message(
                mtype=NotificationType.Plugin,
                title="【插件】媒体刮削更新运行结束",
                text=f"媒体库：【{StorageSchema(self._target_type).name}】{self._target_path}\n更新入库时间晚于 {date} 的文件\n运行耗时：{waste_time}\n更新任务数：{len(msgs)}"
                + "\n\n更新文件列表：\n"
                + "\n".join(msgs)
                if self._detail_notify
                else "",
            )

    def __list_files(
        self,
        storage_type: str,
        starge_path: str,
    ) -> Generator[Tuple[FileItem, str], Any, None]:
        date = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(time.time() - 86400 * self._days)
        )
        files = self.storagechain.list_files(
            FileItem(storage=storage_type, path=starge_path), True
        )
        if files is None or len(files) == 0:
            logger.error(f"未找到文件：【{storage_type}】{starge_path}")
            return
        else:
            for file in files:
                if (
                    file.type == "file"
                    and file.extension is not None
                    and f".{file.extension.lower()}" in settings.RMT_MEDIAEXT
                    and file.path is not None
                ):
                    history = self.transferhis.get_by_dest(dest=file.path)
                    if (
                        history.dest_storage == self._target_type
                        and history.date >= date
                    ):
                        yield (file, str(history.date))

    def stop_service(self) -> None:
        """
        退出插件
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown()
                    self._event.clear()
                self._scheduler = None
        except Exception as e:
            print(str(e))

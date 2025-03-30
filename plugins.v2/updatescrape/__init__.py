import time
from datetime import datetime, timedelta
from threading import Event
from typing import List, Tuple, Dict, Any, Optional, Generator

import pytz
from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore

from app.core.config import settings
from app.api.endpoints.media import scrape
from app.chain.storage import StorageChain
from app.schemas import FileItem, NotificationType
from app.schemas.types import StorageSchema
from app.db.transferhistory_oper import TransferHistoryOper
from app.log import logger
from app.plugins import _PluginBase


class UpdateScrape(_PluginBase):
    # 插件名称
    plugin_name = "媒体库刮削更新"
    # 插件描述
    plugin_desc = "从数据库中获取近期已成功整理视频的信息，补全缺失信息。"
    # 插件图标
    plugin_icon = "scraper.png"
    # 插件版本
    plugin_version = "0.0.1"
    # 插件作者
    plugin_author = "Akimio521"
    # 作者主页
    author_url = "https://github.com/Akimio521"
    # 插件配置项ID前缀
    plugin_config_prefix = "updatescrape_"
    # 加载顺序
    plugin_order = 7
    # 可使用的用户级别
    user_level = 1

    # 私有属性
    transferhis = TransferHistoryOper()
    storagechain = StorageChain()

    _scheduler: BackgroundScheduler | None = None
    _enabled: bool = False  # 运行状态

    _onlyonce: bool  # 立即运行
    _notify: bool  # 通知推送

    _days: int  # 重新刮削几天内入库的文件
    _target_type: str  # 媒体库类型
    _target_path: str  # 媒体库路径

    _event = Event()  # 退出事件

    def init_plugin(self, config: Optional[Dict[str, Any]] = None) -> None:
        logger.warning(f"初始化插件：媒体库刮削更新({self.plugin_version})")
        # 读取配置
        if config:
            self._onlyonce = config.get("onlyonce") or False
            self._notify = config.get("notify") or False
            self._days = int(config.get("days") or 7)
            self._target_type = config.get("target_type") or StorageSchema.Local.value
            self._target_path = config.get("target_path") or ""

        # 停止现有任务
        self.stop_service()

        # 立即运行一次
        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.__update_scrape,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ))
                + timedelta(seconds=3),
                name="重新整理媒体库",
            )
            # 关闭一次性开关
            self._onlyonce = False
            self.update_config(
                {
                    "onlyonce": self._onlyonce,
                    "notify": self._notify,
                    "days": self._days,
                    "target_type": self._target_type,
                    "target_path": self._target_path,
                }
            )
            if self._scheduler.get_jobs():
                # 启动服务
                self._scheduler.print_jobs()
                self._scheduler.start()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
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
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "days",
                                            "label": "更新刮削几天内入库的文件",
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
                                            "label": "新媒体库类型",
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
                                            "placeholder": "需要更新刮削的媒体库路径",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {"enabled": False, "mode": "", "transfer_paths": "", "err_hosts": ""}

    def get_page(self):
        pass

    def __update_scrape(self) -> None:
        """
        媒体库刮削更新
        """
        self._enabled = True
        __c: Dict[str, str | bool] = {
            "通知推送": self._notify,
            "更新刮削几天内入库的文件": f"{self._days} 天",
            "媒体库": f"【{StorageSchema(self._target_type).name}】{self._target_path}",
        }
        logger.info(f"开始媒体库刮削更新，立即运行一次，配置：{__c}")

        scrape_msgs: List[str] = []
        skip_msgs: List[str] = []
        start_time = time.time()
        date = datetime.now(tz=pytz.timezone(settings.TZ)) - timedelta(
            days=self._days
        )  # 在这之后的记录都需要重新刮削
        for file in self.__list_files(self._target_type, self._target_path):
            if self._event.is_set():
                logger.info("媒体库刮削更新服务已停止！")
                self._enabled = False
                return

            if not file.path:
                logger.error(f"文件路径为空，跳过：{file}")
                continue

            history = self.transferhis.get_by_dest(dest=file.path)
            if not history:
                skip_msgs.append(
                    f"【{StorageSchema(self._target_type).name}】{file.path}：未找到整理记录"
                )
                continue
            if history.dest_storage != self._target_type:
                skip_msgs.append(
                    f"【{StorageSchema(self._target_type).name}】{file.path}：整理记录存储类型不匹配"
                )
                continue

            logger.info(
                f"找到整理记录：{history.dest_storage} {history.dest} | {history.date}({type(history.date)})"
            )

            if history.date < date:
                continue

            logger.info(f"文件信息：{file}")
            scrape(file, self._target_type)
            scrape_msgs.append(
                f"【{StorageSchema(self._target_type).name}】{file.path}（入库时间：{history.date}）：更新刮削完成"
            )

        msg: List[str] = [
            f"成功整理 {len(scrape_msgs)} 条",
            f"跳过整理 {len(skip_msgs)} 条",
            f"总耗时 {((time.time() - start_time) / 60):.2f} 分钟",
            "更新文件：",
            *scrape_msgs,
        ]
        if self._notify:
            self.post_message(
                mtype=NotificationType.Plugin,
                title="【插件】媒体库刮削更新完成",
                text="\n".join(msg),
            )
        msg.extend(
            [
                "跳过信息：",
                *skip_msgs,
            ]
        )
        logger.info(f"媒体库刮削更新完成，{'；'.join(msg)}。")

    def __list_files(
        self,
        storage_type: str,
        starge_path: str,
    ) -> Generator[FileItem]:
        file = FileItem(storage=storage_type, path=starge_path)
        files = self.storagechain.list_files(file, True)
        if files is None or len(files) == 0:
            logger.error(f"未找到文件：【{storage_type}】{starge_path}")
            return
        else:
            for f in files:
                if (
                    f.type == "file"
                    and f.extension
                    and f".{f.extension.lower()}" in settings.RMT_MEDIAEXT
                ):
                    yield f

    def stop_service(self):
        """
        退出插件
        """
        logger.warning("正在退出：媒体库刮削更新")
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

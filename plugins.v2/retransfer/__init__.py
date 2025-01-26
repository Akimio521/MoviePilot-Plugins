from datetime import datetime, timedelta
from threading import Event
from typing import List, Tuple, Dict, Any, Optional, Generator
import time

import pytz
from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore


from app.api.endpoints.transfer import manual_transfer
from app.core.config import settings
from app.chain.storage import StorageChain
from app.schemas import ManualTransferItem, Response, FileItem, NotificationType
from app.schemas.types import StorageSchema
from app.db.transferhistory_oper import TransferHistoryOper
from app.log import logger
from app.plugins import _PluginBase


class ReTransfer(_PluginBase):
    # 插件名称
    plugin_name = "重新整理"
    # 插件描述
    plugin_desc = "从数据库中获取已成功整理视频的信息，重新整理到指定目录。"
    # 插件图标
    plugin_icon = "directory.png"
    # 插件版本
    plugin_version = "0.8-1"
    # 插件作者
    plugin_author = "Akimio521"
    # 作者主页
    author_url = "https://github.com/Akimio521"
    # 插件配置项ID前缀
    plugin_config_prefix = "retransfer_"
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
    _background: bool  # 后台转移
    _notify: bool  # 完成通知

    _transfer_type: str  # 转移模式
    _scrape: bool  # 是否刮削
    _library_type_folder: bool  # 是否按类型建立文件夹
    _library_category_folder: bool  # 是否按分类建立文件夹
    _source_type: str  # 原媒体库类型
    _source_path: str  # 原媒体库路径
    _target_type: str  # 目标媒体库类型
    _target_path: str  # 新媒体库路径

    _event = Event()  # 退出事件

    def init_plugin(self, config: Optional[Dict[str, Any]] = None):
        # 读取配置
        if config:
            self._onlyonce = config.get("onlyonce") or False
            self._background = config.get("background") or True
            self._notify = config.get("notify") or False
            self._transfer_type = config.get("transfer_type") or "copy"
            self._scrape = config.get("scrape") or False
            self._library_type_folder = config.get("library_type_folder") or False
            self._library_category_folder = (
                config.get("library_category_folder") or False
            )
            self._source_type = config.get("source_type") or StorageSchema.Local.value
            self._source_path = config.get("source_path") or ""
            self._target_type = config.get("target_type") or StorageSchema.Local.value
            self._target_path = config.get("target_path") or ""

        # 停止现有任务
        self.stop_service()

        # 立即运行一次
        if self._onlyonce:
            __c = {
                "后台转移": self._background,
                "转移模式": self._transfer_type,
                "是否刮削": self._scrape,
                "是否按类型建立文件夹": self._library_type_folder,
                "是否按分类建立文件夹": self._library_category_folder,
                "原媒体库类型": self._source_type,
                "原媒体库路径": self._source_path,
                "新媒体库类型": self._target_type,
                "新媒体库路径": self._target_path,
            }
            logger.info(f"重新整理媒体库服务，立即运行一次，配置：{__c}")
            self._enabled = True
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.__re_transfer,
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
                    "transfer_type": self._transfer_type,
                    "scrape": self._scrape,
                    "library_type_folder": self._library_type_folder,
                    "library_category_folder": self._library_category_folder,
                    "source_type": self._source_type,
                    "source_path": self._source_path,
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
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "onlyonce",
                                            "label": "立即运行一次",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify",
                                            "label": "完成通知",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "background",
                                            "label": "后台转移",
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
                                            "model": "transfer_type",
                                            "label": "转移模式",
                                            "items": [
                                                {"title": "复制", "value": "copy"},
                                                {"title": "移动", "value": "move"},
                                                {"title": "硬链接", "value": "link"},
                                                {
                                                    "title": "软链接",
                                                    "value": "softlink",
                                                },
                                            ],
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
                                            "model": "scrape",
                                            "label": "是否刮削",
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
                                            "model": "library_type_folder",
                                            "label": "是否按类型建立文件夹",
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
                                            "model": "library_category_folder",
                                            "label": "是否按分类建立文件夹",
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
                                            "model": "source_type",
                                            "label": "源文件存储储类型",
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
                                            "model": "source_path",
                                            "label": "源文件路径",
                                            "rows": 1,
                                            "placeholder": "需要重新整理的目录路径",
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
                                            "label": "新媒体库路径",
                                            "rows": 1,
                                            "placeholder": "目标目录路径",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {"enabled": False, "mode": "", "transfer_paths": "", "err_hosts": ""}

    def get_page(self) -> List[dict]:
        pass

    def __re_transfer(self):
        """
        开始重新整理媒体库
        """
        if not self._source_path or not self._target_path:
            logger.error(f"重新整理媒体库服务配置错误！")
            self._enabled = False
            return

        start_time = time.time()
        error_count = 0
        sucess_count = 0

        for file in self.__list_files(self._source_type, self._source_path):
            if self._event.is_set():
                logger.info("重新整理服务已停止！")
                self._enabled = False
                return
            history = self.transferhis.get_by_src(
                src=file.path, storage=self._source_type
            )
            if not history:
                logger.warning(f"【{self._source_type}】{file.path}未找到整理记录！")
                continue

            transer_item = ManualTransferItem(
                logid=history.id,
                target_storage=self._target_type,
                transfer_type=self._transfer_type,
                target_path=self._target_path,
                min_filesize=0,
                scrape=self._scrape,
                library_type_folder=self._library_type_folder,
                library_category_folder=self._library_category_folder,
                from_history=True,
            )
            response: Response = manual_transfer(
                transer_item=transer_item, background=self._background
            )
            if response.success:
                sucess_count += 1
            else:
                error_count += 1
                logger.warning(f"{history.src}重新整理失败：{response.message}")

        msg: List[str] = [
            f"成功整理 {sucess_count} 条",
            f"失败整理 {error_count} 条",
            f"总耗时 {(time.time()-start_time) / 60 :2f} 分钟",
        ]
        logger.info(f"重新整理完成，{'、'.join(msg)}。")
        if self._notify:
            self.post_message(
                mtype=NotificationType.Plugin,
                title="【插件】重新整理完成",
                text="\n".join(msg),
            )
        self._enabled = False

    def __list_files(
        self,
        storage_type: str,
        starge_path: str,
    ) -> Generator[FileItem, None, None]:
        file = FileItem(storage=storage_type, path=starge_path)
        files = self.storagechain.list_files(file, True)
        if not files:
            return None
        for f in files:
            if (
                f.type
                and f.type.lower() == "file"
                and f.extension
                and f".{f.extension.lower()}" in settings.RMT_MEDIAEXT
            ):
                yield f
            else:
                logger.debug(f"忽略文件：【{storage_type}】{f.path}")

    def stop_service(self):
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

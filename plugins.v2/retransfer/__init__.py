from datetime import datetime, timedelta
from threading import Event
from typing import List, Tuple, Dict, Any, Optional, Generator
import time

import pytz
from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore


from app.api.endpoints.transfer import manual_transfer
from app.core.config import settings
from app.chain.storage import StorageChain
from app.schemas import ManualTransferItem, Response, FileItem
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
    plugin_version = "0.6"
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
    # 限速开关
    _enabled: bool
    _onlyonce: bool
    _background: bool  # 后台刮削

    _transfer_type: str  # 转移模式
    _scrape: bool  # 是否刮削
    _library_type_folder: bool  # 是否按类型建立文件夹
    _library_category_folder: bool  # 是否按分类建立文件夹
    _source_type: str  # 原媒体库类型
    _source_path: str  # 原媒体库路径
    _target_type: str  # 目标媒体库类型
    _target_path: str  # 新媒体库路径
    # 退出事件
    _event = Event()

    def init_plugin(self, config: Optional[Dict[str, Any]] = None):

        # 读取配置
        if config:
            self._enabled = config.get("enabled") or False
            self._onlyonce = config.get("onlyonce") or False
            self._background = config.get("background") or True
            self._transfer_type = config.get("transfer_type") or "copy"
            self._scrape = config.get("scrape") or True
            self._library_type_folder = config.get("library_type_folder") or True
            self._library_category_folder = (
                config.get("library_category_folder") or True
            )
            self._source_type = config.get("source_type") or StorageSchema.Local.value
            self._source_path = config.get("source_path") or ""
            self._target_type = config.get("target_type") or StorageSchema.Local.value
            self._target_path = config.get("target_path") or ""

        # 停止现有任务
        self.stop_service()

        # 立即运行一次
        if self._enabled and self._onlyonce:

            if self._onlyonce:
                __c = {
                    "后台刮削": self._background,
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
                        "onlyonce": False,
                        "enabled": self._enabled,
                        "background": self._background,
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
                                            "model": "enabled",
                                            "label": "启用插件",
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
                                            "model": "background",
                                            "label": "后台刮削",
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
        开始刮削媒体库
        """
        if not all(
            [
                self._source_type,
                self._source_path,
                self._target_type,
                self._target_path,
            ]
        ):
            logger.error(f"重新整理媒体库服务配置错误！")
            return

        start_time = time.time()
        error_count = 0
        sucess_count = 0

        for file in self.__list_files(self._source_type, self._source_path):
            history = self.transferhis.get_by_src(
                src=file.path, storage=self._source_type
            )
            if not history:
                logger.warning(f"【{self._source_type}】{file.path}未找到整理记录！")
                continue

            history.id
            target_storage = history.dest_storage
            transer_item = ManualTransferItem(
                logid=history.id,
                target_storage=target_storage,
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

        logger.info(
            f"重新整理完成。成功{sucess_count}条，失败{error_count}条！总耗时{(time.time()-start_time) / 60 :2f}分钟"
        )

    def __list_files(
        self,
        storage_type: str,
        starge_path: str,
    ) -> Generator[FileItem, None, None]:
        file = FileItem(storage=storage_type, path=starge_path)
        files = self.storagechain.list_files(file, True)
        # print(files)
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
                print(f"跳过：{f}")

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

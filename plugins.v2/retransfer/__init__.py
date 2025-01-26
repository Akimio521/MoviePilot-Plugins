from datetime import datetime, timedelta
from pathlib import Path
from threading import Event
from typing import List, Tuple, Dict, Any,Optional
import time

import pytz
from apscheduler.schedulers.background import BackgroundScheduler


from app.api.endpoints.transfer import manual_transfer
from app.core.config import settings
from app.schemas import  ManualTransferItem,Response
from app.db.transferhistory_oper import TransferHistoryOper
from app.db.models.transferhistory import TransferHistory
from app.log import logger
from app.plugins import _PluginBase
from app.utils.system import SystemUtils
from app.utils.http import RequestUtils


class ReTransfer(_PluginBase):
    # 插件名称
    plugin_name = "重新整理"
    # 插件描述
    plugin_desc = "从数据库中获取已成功整理视频的信息，重新整理到指定目录。"
    # 插件图标
    plugin_icon = "directory.png"
    # 插件版本
    plugin_version = "0.4"
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
    transferhis = None

    req = None
    
    _scheduler = None
    # 限速开关
    _enabled = False
    _onlyonce = False

    _transfer_type = "copy" # 转移模式
    _scrape = True # 是否刮削
    _library_type_folder = True # 是否按类型建立文件夹
    _library_category_folder = True # 是否按分类建立文件夹
    _source_path = "" # 原媒体库路径
    _target_path = "" # 新媒体库路径
    # 退出事件
    _event = Event()

    def init_plugin(self, config: Optional[Dict[str,Any]] = None):
        
        # 读取配置
        if config:
            self._enabled = config.get("enabled")
            self._onlyonce = config.get("onlyonce")
            self._transfer_type = config.get("transfer_type") or "copy"
            self._scrape = config.get("scrape") or True
            self._library_type_folder = config.get("library_type_folder") or True
            self._library_category_folder = config.get("library_category_folder") or True
            self._source_path = config.get("source_path") or ""
            self._target_path = config.get("target_path") or ""

        # 停止现有任务
        self.stop_service()

        # 立即运行一次
        if self._enabled and self._onlyonce:
            self.transferhis = TransferHistoryOper()
            self.req = RequestUtils()

            if self._onlyonce:
                __c = {
                    "转移模式": self._transfer_type,
                    "是否刮削": self._scrape,
                    "是否按类型建立文件夹": self._library_type_folder,
                    "是否按分类建立文件夹": self._library_category_folder,
                    "原媒体库路径": self._source_path,
                    "新媒体库路径": self._target_path
                }
                logger.info(f"重新整理媒体库服务，立即运行一次，配置：{__c}")
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                self._scheduler.add_job(func=self.__re_transfer, trigger='date',
                                        run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                        name="重新整理媒体库")
                # 关闭一次性开关
                self._onlyonce = False
                self.update_config({
                    "onlyonce": False,
                    "enabled": self._enabled,
                    "transfer_type": self._transfer_type,
                    "scrape": self._scrape,
                    "library_type_folder": self._library_type_folder,
                    "library_category_folder": self._library_category_folder,
                    "source_path": self._source_path,
                    "target_path": self._target_path
                })
                if self._scheduler.get_jobs():
                    # 启动服务
                    self._scheduler.print_jobs()
                    self._scheduler.start()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_service(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'transfer_type',
                                            'label': '转移模式',
                                            'items': [
                                                {'title': '复制', 'value': 'copy'},
                                                {'title': '移动', 'value': 'move'},
                                                {'title': '硬链接', 'value': 'link'},
                                                {'title': '软链接', 'value': 'softlink'},
                                            ]
                                        }
                                    }
                                ]
                            },{
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'scrape',
                                            'label': '是否刮削',
                                        }
                                    }
                                ]
                            },{
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'library_type_folder',
                                            'label': '是否按类型建立文件夹',
                                        }
                                    }
                                ]
                            },{
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 3
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'library_category_folder',
                                            'label': '是否按分类建立文件夹',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'source_path',
                                            'label': '重新整理路径',
                                            'rows': 2,
                                            'placeholder': '一个目录'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'target_path',
                                            'label': '目标媒体库路径',
                                            'rows': 2,
                                            'placeholder': '一个目录'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "mode": "",
            "transfer_paths": "",
            "err_hosts": ""
        }

    def get_page(self) -> List[dict]:
        pass

    def __re_transfer(self):
        """
        开始刮削媒体库
        """
        if not self._source_path:
            logger.error(f"未设置需要重新整理媒体库的路径")
            return
        if not self._target_path:
            logger.error(f"未设置目标媒体库路径")
            return
        
        start_time = time.time()
        paths = SystemUtils.list_files(Path(self._source_path), settings.RMT_MEDIAEXT)
        total = len(paths)
        error_count = 0
        sucess_count = 0
        
        for path in paths:
            history:TransferHistory = self.transferhis.get_by_src(str(path))
            if  not history:
                total -= 1
                continue

            history.id
            target_storage = history.dest_storage
            transer_item = ManualTransferItem(logid=history.id, 
                            target_storage=target_storage,
                            transfer_type=self._transfer_type, 
                            target_path=self._target_path,
                            min_filesize=0,
                            scrape=self._scrape, 
                            library_type_folder=self._library_type_folder, library_category_folder=self._library_category_folder,
                            from_history=True,
            )
            response:Response = manual_transfer(transer_item=transer_item, background=False)
            if response.success:
                sucess_count += 1
            else:
                error_count += 1
                logger.warning(f"{history.src}重新整理失败：{response.message}")

        logger.info(f"共有{total}条记录，成功{sucess_count}条，失败{error_count}条！总耗时{(time.time()-start_time) / 60 :2f}分钟")


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
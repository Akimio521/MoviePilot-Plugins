from datetime import datetime, timedelta
from pathlib import Path
from threading import Event
from typing import List, Tuple, Dict, Any,Optional
import time

import pytz
from apscheduler.schedulers.background import BackgroundScheduler


from app.schemas import ManualTransferItem, EpisodeFormat
from app.schemas.file import FileItem
from app.chain.transfer import TransferChain
from app.chain.storage import StorageChain
from app.core.config import settings
from app.core.metainfo import MetaInfoPath
from app.db.transferhistory_oper import TransferHistoryOper
from app.db.models.transferhistory import TransferHistory
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import MediaType
from app.utils.system import SystemUtils


class ReTransfer(_PluginBase):
    # 插件名称
    plugin_name = "重新整理"
    # 插件描述
    plugin_desc = "从数据库中获取已成功整理视频的信息，重新整理到指定目录。"
    # 插件图标
    plugin_icon = "directory.png"
    # 插件版本
    plugin_version = "0.3"
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
    storagechain = None
    transferchain = None
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
            self.storagechain = StorageChain()
            self.transferchain = TransferChain()

            if self._onlyonce:
                logger.info(f"重新整理媒体库服务，立即运行一次，配置：",{
                    "转移模式": self._transfer_type,
                    "是否刮削": self._scrape,
                    "是否按类型建立文件夹": self._library_type_folder,
                    "是否按分类建立文件夹": self._library_category_folder,
                    "原媒体库路径": self._source_path,
                    "新媒体库路径": self._target_path
                })
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
            history: TransferHistory = self.transferhis.get_by_src(str(path))
            if  not history:
                total -= 1
                continue

            # 重新整理
            if history.status and ("move" in history.mode):# 重新整理成功的转移，则使用成功的 dest 做 in_path
                src_fileitem = FileItem(**history.dest_fileitem)
            else:
                # 源路径
                src_fileitem = FileItem(**history.src_fileitem)
                # 目的路径
                if history.dest_fileitem:
                    # 删除旧的已整理文件
                    dest_fileitem = FileItem(**history.dest_fileitem)
                    state = self.storagechain.delete_media_file(dest_fileitem, mtype=MediaType(history.type))
                    if not state:
                        return logger.warning(f"删除已整理文件失败：{dest_fileitem.storage} {dest_fileitem.path}")
            transer_item = ManualTransferItem()
            transer_item.type_name = str(history.type) if history.type else transer_item.type_name
            transer_item.tmdbid = int(history.tmdbid) if history.tmdbid else transer_item.tmdbid
            transer_item.doubanid = str(history.doubanid) if history.doubanid else transer_item.doubanid
            transer_item.season = int(str(history.seasons).replace("S", "")) if history.seasons else transer_item.season
            if history.episodes:
                if "-" in str(history.episodes):# E01-E03多集合并
                    episode_start, episode_end = str(history.episodes).split("-")
                    episode_list: List[int] = []
                    for i in range(int(episode_start.replace("E", "")), int(episode_end.replace("E", "")) + 1):
                        episode_list.append(i)
                    transer_item.episode_detail = ",".join(str(e) for e in episode_list)
                else:# E01单集
                    transer_item.episode_detail = str(history.episodes).replace("E", "")
            
            # 类型
            mtype = MediaType(transer_item.type_name) if transer_item.type_name else None
            # 自定义格式
            epformat = None
            if transer_item.episode_offset or transer_item.episode_part \
                    or transer_item.episode_detail or transer_item.episode_format:
                epformat = EpisodeFormat(
                    format=transer_item.episode_format,
                    detail=transer_item.episode_detail,
                    part=transer_item.episode_part,
                    offset=transer_item.episode_offset,
                )
            state, errormsg = self.transferchain.manual_transfer(
                fileitem=src_fileitem,
                target_storage=transer_item.target_storage,
                target_path=self._target_path,
                tmdbid=transer_item.tmdbid,
                doubanid=transer_item.doubanid,
                mtype=mtype,
                season=transer_item.season,
                epformat=epformat,
                scrape=self._scrape,
                library_type_folder=self._library_type_folder,
                library_category_folder=self._library_category_folder,
                force=True,
                background=True
            )
            if not state:
                logger.error(f"重新整理失败：{src_fileitem.storage} {src_fileitem.path} {errormsg}")
                error_count += 1
            else:
                logger.debug(f"重新整理成功：{src_fileitem.storage} {src_fileitem.path}")
                sucess_count += 1
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
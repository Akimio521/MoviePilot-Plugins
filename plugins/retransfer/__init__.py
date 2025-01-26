from typing import Any, Dict, List, Tuple
from pathlib import Path
import time

from app.core.config import settings
from app.log import logger
from app.plugins import _PluginBase
from app.chain.transfer import TransferChain
from app.db.transferhistory_oper import TransferHistoryOper
from app.db.models.transferhistory import TransferHistory
from app.utils.system import SystemUtils
from app import schemas
from app.schemas import MediaType


class ReTransfer(_PluginBase):
    # 插件名称
    plugin_name = "重新整理"
    # 插件描述
    plugin_desc = "从数据库中获取已成功整理视频的信息，重新整理到指定目录。"
    # 插件图标
    plugin_icon = "directory.png"
    # 插件版本
    plugin_version = "0.9"
    # 插件作者
    plugin_author = "Akimio521"
    # 作者主页
    author_url = "https://github.com/Akimio521"
    # 插件配置项ID前缀
    plugin_config_prefix = "retransfer_"
    # 加载顺序
    plugin_order = 4
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    transferhis = None
    transfer = None
    # 每轮处理数量
    _batch_size: int = 50

    # 立即运行
    _onlyonce: bool = False
    # 是否刮削
    _scrape: bool = False
    # 转移方式
    _transfer_type: str | None = None
    # 源目录
    _source_path: str | None = None
    # 目标目录
    _target_path: str | None = None

    def init_plugin(self, config: dict):
        """
        初始化插件
        """
        if not config:
            return

        self._onlyonce = config.get("onlyonce", False)
        self._scrape = config.get("scrape", False)
        self._transfer_type = config.get("transfer_type", settings.TRANSFER_TYPE)
        self._source_path = config.get("source_path")
        self._target_path = config.get("target_path")
        self.transferhis = TransferHistoryOper()
        self.transfer = TransferChain()

        if self._onlyonce:
            self._onlyonce = False
            self.__task()

        self.__update_config()

    def __task(self):
        """
        重新整理具体执行任务
        """
        # 重新整理
        if not self._source_path or not self._target_path:
            logger.error("请设置源目录和目标目录")
            return

        start_time = time.time()
        logger.info("开始重新整理...")
        err_num = 0
        continue_num = 0
        paths = SystemUtils.list_files(Path(self._source_path), settings.RMT_MEDIAEXT)
        total = len(paths)
        logger.info(f"共有{total}条记录")
        for path in paths:
            history: TransferHistory = self.transferhis.get_by_src(str(path))
            type_name = None
            tmdbid = None
            doubanid = None
            season = None
            episode_detail = None
            mtype = None

            if not history.status:
                continue_num += 1
                continue

            # 源路径
            in_path = Path(history.src)
            # 目的路径
            if history.dest and str(history.dest) != "None":
                # 删除旧的已整理文件
                self.transfer.delete_files(Path(history.dest))

            if history.type:
                type_name = str(history.type)

            if history.tmdbid:
                tmdbid = int(history.tmdbid)
            elif history.doubanid:
                doubanid = str(history.doubanid)

            if history.seasons:
                season = int(str(history.seasons).replace("S", ""))

            if history.episodes:
                if "-" in str(history.episodes):
                    # E01-E03多集合并
                    episode_start, episode_end = str(history.episodes).split("-")
                    episode_list: list[int] = []
                    for i in range(
                        int(episode_start.replace("E", "")),
                        int(episode_end.replace("E", "")) + 1,
                    ):
                        episode_list.append(i)
                    episode_detail = ",".join(str(e) for e in episode_list)
                else:
                    # E01单集
                    episode_detail = str(history.episodes).replace("E", "")

            if type_name:
                mtype = MediaType(type_name)

            epformat = schemas.EpisodeFormat(
                format=None,
                detail=episode_detail,
                part=None,
                offset=None,
            )

            # 开始转移
            state, errormsg = self.transfer.manual_transfer(
                storage="local",
                in_path=in_path,
                drive_id=None,
                fileid=None,
                filetype="file",
                target=Path(self._target_path),
                tmdbid=tmdbid,
                doubanid=doubanid,
                mtype=mtype,
                season=season,
                transfer_type=self._transfer_type,
                epformat=epformat,
                min_filesize=0,
                scrape=self._scrape,
                force=True,
            )
            # 失败
            if not state:
                if isinstance(errormsg, list):
                    for msg in errormsg:
                        logger.error("整理失败，出错信息:", msg)
                    err_num += len(errormsg)

        logger.info(
            f"重新整理完成，共有{total-continue_num-err_num}个文件整理成功，共有{continue_num}个文件跳过，共有{err_num}个文件整理失败！总耗时{(time.time()-start_time) / 60 :2f}分钟"
        )

    def __update_config(self):
        """
        更新配置
        """
        self.update_config(
            {
                "onlyonce": self._onlyonce,
                "scrape": self._scrape,
                "transfer_type": self._transfer_type,
                "source_path": self._source_path,
                "target_path": self._target_path,
            }
        )

    def get_state(self) -> Dict[str, Any]:
        """
        获取插件的当前状态。
        """
        return self._onlyonce

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
                                            "model": "scrape",
                                            "label": "刮削元数据",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "transfer_type",
                                            "label": "整理方式",
                                            "items": [
                                                {"title": "移动", "value": "move"},
                                                {"title": "复制", "value": "copy"},
                                                {"title": "硬链接", "value": "link"},
                                                {
                                                    "title": "软链接",
                                                    "value": "softlink",
                                                },
                                                {
                                                    "title": "Rclone复制",
                                                    "value": "rclone_copy",
                                                },
                                                {
                                                    "title": "Rclone移动",
                                                    "value": "rclone_move",
                                                },
                                            ],
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
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "source_path",
                                            "label": "源目录",
                                            "rows": 1,
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "target_path",
                                            "label": "目标目录",
                                            "rows": 1,
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "scrape": True,
            "transfer_type": settings.TRANSFER_TYPE,
            "source_path": "",
            "target_path": "",
        }

    def get_page(self) -> List[Dict]:
        pass

    def stop_service(self):
        """
        退出插件
        """
        pass

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        注册插件命令
        """
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        """
        注册插件API接口
        """
        pass

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        """
        pass

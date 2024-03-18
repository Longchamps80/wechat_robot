import csv
import json
import os
import uvicorn
import asyncio
import aiofiles
import logging
from datetime import datetime, date
from fastapi import FastAPI, Body, HTTPException
from pydantic import BaseModel
import xml.etree.ElementTree as ET


class Msg(BaseModel):
    id: int
    ts: int
    sign: str
    type: int
    xml: str
    sender: str
    roomid: str
    content: str
    thumb: str
    extra: str
    is_at: bool
    is_self: bool
    is_group: bool


class Config:
    def __init__(self, config_path="config.json"):
        with open(config_path, "r", encoding="utf-8") as config_file:
            config_data = json.load(config_file)
        self.accepted_types = set(config_data.get("accepted_types", []))
        self.chatroom_id_to_name = config_data.get("chatroom_id_to_name", {})
        self.log_level = config_data.get("log_level", "INFO")
        self.host = config_data.get("host", "127.0.0.1")
        self.port = config_data.get("port", 8001)
        self.file_encoding = config_data.get("file_encoding", "utf-8-sig")
        self.file_write_mode = config_data.get("file_write_mode", "a")
        self.date_format = config_data.get("date_format", "%Y%m%d%H")


config = Config()


class FileManager:
    @staticmethod
    async def write_to_csv(file_name, header, data):
        async with aiofiles.open(file_name, config.file_write_mode, newline='',
                                 encoding=config.file_encoding) as file:
            writer = csv.writer(file)
            if (await file.tell()) == 0:
                await writer.writerow(header)
            await writer.writerow(data)


async def read_group_ids(file_path):
    try:
        async with aiofiles.open(file_path, mode='r',
                                 encoding='utf-8') as file:
            return [row[0] for row in csv.reader(file) if row]
    except FileNotFoundError:
        return []


async def parse_xml_content(content):
    try:
        root = ET.fromstring(content)
        title_element = root.find('.//title')
        return title_element.text if title_element is not None else "无标题"
    except ET.ParseError:
        return "XML解析错误"


async def save_message_to_csv(message: Msg):
    try:
        if message.type not in config.accepted_types or message.roomid not in config.chatroom_id_to_name:
            return

        room_name = config.chatroom_id_to_name[message.roomid]
        current_time = datetime.now()
        folder_path = f"./{room_name}/{current_time.strftime('%Y%m%d')}/"  # Specify the base folder path

        if not os.path.exists(folder_path):
            os.makedirs(folder_path)  # Create the folder if it doesn't exist

        file_name = f"{folder_path}messages_{current_time.strftime('%Y%m%d%H')}.csv"

        header = ['type', 'sender', 'content', 'extra'] if not (
                    message.type == 49 and message.is_group) else ['type',
                                                                   'sender',
                                                                   'title',
                                                                   'content',
                                                                   'extra']
        title_content = await parse_xml_content(
            message.content) if message.type == 49 and message.is_group else ""
        data = [str(message.type), message.sender,
                title_content if title_content else message.content,
                message.extra]

        await FileManager.write_to_csv(file_name, header, data)
        log_and_print("[INFO] Message saved", room_name, message)

    except FileNotFoundError:
        log_and_print("[ERROR] File not found", room_name, message)
    except csv.Error as csv_error:
        log_and_print(f"[ERROR] CSV error: {csv_error}", room_name, message)
    except Exception as e:
        log_and_print(f"[ERROR] An error occurred: {e}", room_name, message)


async def msg_cb(msg: Msg = Body(...)):
    try:
        await save_message_to_csv(msg)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"status": 0, "message": "成功"}


def log_and_print(log_message, room_name, message):
    log_function = getattr(logging, config.log_level.upper())
    logging.log(log_function, log_message)

    if "Message saved" in log_message:
        log_details = f"[{config.log_level}] {datetime.now()} - {room_name} - {message.type} - {message.content}"
        print(log_details)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app = FastAPI()
    app.add_api_route("/callback", msg_cb, methods=["POST"])
    uvicorn.run(app, host=config.host, port=config.port)


#!/usr/bin/env python
import sys
import shutil
from typing import Optional, List, Tuple, Dict
import re

import typer
from rich import print
from rich.columns import Columns
from rich.console import Console
from rich.traceback import install
from rich.panel import Panel
from rich.text import Text

# fmt: off
# Mapping from topics to colors
TOPICS = {
    "TIMR": "#9a9a99",
    "VOTE": "#67a0b2",
    "LEAD": "#d0b343",
    "TERM": "#70c43f",
    "LOG1": "#4878bc",
    "LOG2": "#398280",
    "CMIT": "#98719f",
    "PERS": "#d08341",
    "SNAP": "#FD971F",
    "DROP": "#ff615c",
    "CLNT": "#00813c",
    "TEST": "#fe2c79",
    "INFO": "#ffffff",
    "WARN": "#d08341",
    "ERRO": "#fe2626",
    "TRCE": "#fe2626",
}
# fmt: on

def list_topics(value: Optional[str]):
    if value is None:
        return value
    topics = value.split(",")
    for topic in topics:
        if topic not in TOPICS:
            raise typer.BadParameter(f"topic {topic} not recognized")
    return topics

def print_test_message(line: str, console: Console):
    """Print formatted test messages (both success and failure)."""
    # 匹配成功的测试结果
    passed_match = re.match(r'(.*Passed.*?)(\d+\.\d+)(.*?)(\d+)(\s+)(\d+)(\s+)(\d+)(\s+)(\d+)\s*$', line)
    if passed_match:
        # 分解匹配的各个部分并添加颜色
        parts = list(passed_match.groups())
        formatted_text = Text.assemble(
            (parts[0], "green"), # "... Passed" 部分
            (parts[1], "bright_green"), # 时间
            (parts[2], "green"), # 空格等其他字符
            (parts[3], "bright_green"), # 第一个数字
            (parts[4], "green"), # 空格
            (parts[5], "bright_green"), # 第二个数字
            (parts[6], "green"), # 空格
            (parts[7], "bright_green"), # 第三个数字
            (parts[8], "green"), # 空格
            (parts[9], "bright_green"), # 第四个数字
        )
        console.print(formatted_text)
        return True

    # 匹配 PASS 行
    elif line.strip() == "PASS":
        console.print(line, style="bold green")
        return True
    
    # 匹配测试成功的总结行
    elif line.startswith("ok"):
        parts = line.split()
        if len(parts) >= 4:  # 确保有足够的部分可以格式化
            module_path = parts[1]
            time_str = parts[2]
            console.print(Text.assemble(
                ("ok      ", "green"),
                (module_path, "bright_green"),
                ("    ", "green"),
                (time_str, "bright_green")
            ))
        else:
            console.print(line, style="green")
        return True

    # 处理失败的测试信息
    elif line.startswith('FAIL'):
        console.print(line, style="bold red")
        return True
    elif '--- FAIL:' in line:
        match = re.match(r'--- FAIL: (\w+) \(([\d.]+)s\)', line)
        if match:
            test_name, duration = match.groups()
            console.print(Text.assemble(
                ("--- FAIL: ", "bold red"),
                (test_name, "bold red underline"),
                (f" ({duration}s)", "red")
            ))
            return True
    elif 'go' in line:
        console.print(line, style="yellow")
        return True
    
    return False

def main(
    file: typer.FileText = typer.Argument(None, help="File to read, stdin otherwise"),
    colorize: bool = typer.Option(True, "--no-color"),
    n_columns: Optional[int] = typer.Option(None, "--columns", "-c"),
    ignore: Optional[str] = typer.Option(None, "--ignore", "-i", callback=list_topics),
    just: Optional[str] = typer.Option(None, "--just", "-j", callback=list_topics),
):
    topics = list(TOPICS)

    # We can take input from a stdin (pipes) or from a file
    input_ = file if file else sys.stdin
    # Print just some topics or exclude some topics (good for avoiding verbose ones)
    if just:
        topics = just
    if ignore:
        topics = [lvl for lvl in topics if lvl not in set(ignore)]

    topics = set(topics)
    console = Console()
    width = console.size.width

    panic = False
    
    for line in input_:
        try:
            # 首先尝试作为测试消息处理
            if print_test_message(line.strip(), console):
                continue

            # 尝试解析为普通日志条目
            parts = line.strip().split(" ")
            if len(parts) >= 3:
                time, topic, *msg = parts
                # To ignore some topics
                if topic not in topics:
                    continue

                msg = " ".join(msg)

                # Debug calls from the test suite aren't associated with
                # any particular peer
                if topic != "TEST":
                    try:
                        i = int(msg[1])
                    except (IndexError, ValueError):
                        print(line.strip())
                        continue

                # Colorize output by using rich syntax when needed
                if colorize and topic in TOPICS:
                    color = TOPICS[topic]
                    msg = f"[{color}]{msg}[/{color}]"

                # Single column printing. Always the case for debug stmts in tests
                if n_columns is None or topic == "TEST":
                    print(time, msg)
                # Multi column printing
                else:
                    cols = ["" for _ in range(n_columns)]
                    msg = "" + msg
                    cols[i] = msg
                    col_width = int(width / n_columns)
                    cols = Columns(cols, width=col_width - 1, equal=True, expand=True)
                    print(cols)
            else:
                # Handle remaining special messages
                if line.startswith('panic'):
                    panic = True
                    console.print(Panel(line.strip(), style="bold red", title="PANIC"))
                elif not panic and line.strip():
                    print("#" * console.width)
                else:
                    print(line, end="")
        except Exception as e:
            # If parsing fails, print directly
            print(line.strip())

if __name__ == "__main__":
    typer.run(main)
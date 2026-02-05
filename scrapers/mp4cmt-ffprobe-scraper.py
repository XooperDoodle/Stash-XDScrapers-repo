#!/usr/bin/env python3
import json
import sys
import subprocess
import re
from pathlib import Path

__version__ = "1.3"

def debug_print(msg):
    """Write debug messages to stderr as expected by Stash"""
    sys.stderr.write(str(msg) + "\n")

def run_ffprobe(file_path):
    """Run ffprobe to extract format and stream information"""
    try:
        cmd = [
            "ffprobe",
            "-v", "error",
            "-show_format",
            "-show_streams",
            "-of", "json",
            file_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        debug_print(f"ffprobe failed: {e}")
        return None
    except FileNotFoundError:
        debug_print("ffprobe not found. Please ensure it is installed and in your PATH.")
        return None
    except json.JSONDecodeError as e:
        debug_print(f"Failed to parse ffprobe JSON: {e}")
        return None
    except Exception as e:
        debug_print(f"ffprobe unexpected error: {e}")
        return None

def parse_comment(comment_text):
    """Parse the comment field into details, url, tags, and performers"""
    if not comment_text:
        return {"details": "", "url": "", "tags": [], "performers": []}
    
    result = {
        "details": "",
        "url": "",
        "tags": [],
        "performers": []
    }
    
    url_match = re.search(r'(https://[^\s\]]+)', comment_text)
    if url_match:
        result["url"] = url_match.group(1)
    
    tags_section = re.search(r'### Tags ###\s*\n(.*?)(?=\n--|\n\n|$)', comment_text, re.DOTALL)
    if tags_section:
        tags_text = tags_section.group(1)
        tags = [tag.strip() for tag in re.split(r',|\n', tags_text) if tag.strip()]
        result["tags"] = list(set(tag for tag in tags if tag and not tag.startswith('--')))
    
    perf_matches = re.findall(r'__perf-\s*\(([^)]+)\)\s*__', comment_text)
    result["performers"] = [name.strip() for name in perf_matches if name.strip()]
    
    details = comment_text
    if result["url"]:
        if f"#URL-[{result['url']}]" in details:
            details = details.replace(f"#URL-[{result['url']}]", "")
        else:
            details = details.replace(result["url"], "")
    details = re.sub(r'### Tags ###.*$', '', details, flags=re.DOTALL)
    details = re.sub(r'__perf-\s*\([^)]+\)\s*__', '', details)
    details = re.sub(r'\n\s*\n', '\n\n', details.strip())
    result["details"] = details.strip()
    
    return result

def main():
    # Always output valid JSON at the end
    output = {
        "title": None,
        "details": None,
        "url": None,
        "performers": [],
        "tags": []
    }

    # Force UTF-8 for Stash communication
    # Commented out to prevent potential issues on some systems
    # try:
    #     if hasattr(sys.stdin, 'reconfigure'):
    #         sys.stdin.reconfigure(encoding='utf-8')
    #     if hasattr(sys.stdout, 'reconfigure'):
    #         sys.stdout.reconfigure(encoding='utf-8')
    # except Exception as e:
    #     debug_print(f"Failed to reconfigure streams: {e}")

    try:
        try:
            input_content = sys.stdin.read()
            if not input_content:
                # If no input, just print empty output and exit
                debug_print("No input provided to stdin")
                print(json.dumps(output, indent=2))
                sys.stdout.flush()
                return
            input_data = json.loads(input_content)
            debug_print(f"Received input structure: {list(input_data.keys())}")
        except Exception as e:
            debug_print(f"Failed to parse input JSON: {e}")
            print(json.dumps(output, indent=2))
            sys.stdout.flush()
            return

        # Handle different input structures
        file_path = None
        
        # Try scene fragment structure (files array)
        if "files" in input_data and input_data["files"]:
            file_path = input_data["files"][0].get("path")
        # Try direct file structure
        elif "file" in input_data and "path" in input_data["file"]:
            file_path = input_data["file"]["path"]
        
        if not file_path:
            debug_print("No file path found in input")
            print(json.dumps(output, indent=2))
            sys.stdout.flush()
            return
        
        if not Path(file_path).exists():
            debug_print(f"File not found: {file_path}")
            print(json.dumps(output, indent=2))
            sys.stdout.flush()
            return
        
        probe_data = run_ffprobe(file_path)
        if not probe_data:
            print(json.dumps(output, indent=2))
            sys.stdout.flush()
            return
        
        comment_text = None
        if "format" in probe_data and "tags" in probe_data["format"]:
            comment_text = probe_data["format"]["tags"].get("comment")
        
        parsed = None
        if not comment_text:
            debug_print("No comment field found in file")
            parsed = {"details": "", "url": "", "tags": [], "performers": []}
        else:
            parsed = parse_comment(comment_text)
        
        output = {
            "title": None,
            "details": parsed["details"] if parsed["details"] else None,
            "url": parsed["url"] if parsed["url"] else None,
            "performers": [{"name": name} for name in parsed["performers"]],
            "tags": [{"name": name} for name in parsed["tags"]]
        }
        
        print(json.dumps(output, indent=2))
        sys.stdout.flush()

    except Exception as e:
        debug_print(f"Unexpected script error: {e}")
        # Print fallback output so Stash doesn't get EOF
        print(json.dumps(output, indent=2))
        sys.stdout.flush()

if __name__ == "__main__":
    main()
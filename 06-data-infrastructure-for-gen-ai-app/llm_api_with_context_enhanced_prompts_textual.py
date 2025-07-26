########################################################
# นำเข้าไลบรารีที่จำเป็น

########################################################
# ดึง API Key สำหรับ Gemini จาก environment variable
########################################################
# ฟังก์ชันสำหรับเรียกใช้งานโมเดล Gemini เพื่อสร้างคำตอบจาก prompt ที่กำหนด
########################################################
# สร้าง client สำหรับเชื่อมต่อกับ Gemini API
########################################################
# ตัวอย่าง context ที่จะใช้ประกอบการตอบคำถาม
########################################################
# ตัวแปร question สำหรับเก็บคำถามที่ต้องการถามโมเดล
########################################################
# สร้าง prompt ที่รวม context และคำถาม เพื่อส่งให้โมเดล
########################################################
# เรียกใช้งาน Gemini API เพื่อให้โมเดลตอบคำถามตาม context ที่กำหนด
########################################################
# แสดงผลลัพธ์ที่ได้จากโมเดล
########################################################
# สรุปการทำงานของโค้ดนี้
#
# 1. ดึง API Key สำหรับ Gemini จาก environment variable
# 2. สร้าง client สำหรับเชื่อมต่อกับ Gemini API
# 3. นิยามฟังก์ชัน ask_gemini สำหรับส่ง prompt และรับคำตอบจากโมเดล
# 4. กำหนด context และ prompt ที่จะใช้ถามโมเดล
# 5. เรียกใช้ฟังก์ชันเพื่อรับคำตอบและแสดงผลลัพธ์
#
# โค้ดนี้เหมาะสำหรับการสร้างระบบถาม-ตอบที่ใช้ context เฉพาะเจาะจงเพื่อให้โมเดล LLM ตอบคำถามได้ตรงประเด็นมากขึ้น

import os

from google import genai
from google.genai import types


GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
# GEMINI_API_KEY = "YOUR_GEMINI_API_KEY"


def ask_gemini(client, model: str = "gemini-2.0-flash-001", prompt: str = ""):
    response = client.models.generate_content(
        model=model,
        contents=prompt,
    )
    return response.text


# Set up a Gemini client
client = genai.Client(api_key=GEMINI_API_KEY)

context = """
Skooldio offers a full refund within 14 days of booking, provided no services have been consumed.
After 14 days, a 50% refund is possible upon review. Skooldio has a remote work policy but it has to
review on a case by case basis. Let's day if no meeting on that day, the employee can work from home.
"""

question = "What are the benefits of remote work?"

prompt_with_context = f"""
You are a helpful assistant. Use the following context to answer the question.

Context:
{"You are an instructor in data engineering bootcamp that help people to advice the road map to study for becoming a real data engineer"}

Question:
{"Hi can you give me a road map for studying data infrastructure for Gen AI applications?"}
"""
response = ask_gemini(client, prompt=prompt_with_context)

print(response)

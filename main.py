import asyncio
import re
import os
import random
import logging
import urllib.request
import time
import string
from datetime import datetime, timezone, timedelta
from threading import Thread
from flask import Flask, request, jsonify
from telethon import TelegramClient, events, Button as TButton
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError
from supabase import create_client, Client

app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is running!"

def run():
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    t = Thread(target=run)
    t.start()
    
# ---> THÊM: CẤU HÌNH GIỜ VIỆT NAM (GMT+7)
VN_TZ = timezone(timedelta(hours=7))

# ---> THÊM: HÀM TẠO MÃ GIAO DỊCH XỊN SÒ (ORDER ID)
def generate_order_id(prefix="MD"):
    return f"{prefix}-{''.join(random.choices(string.ascii_uppercase + string.digits, k=6))}"

# ---> THÊM: CẤU HÌNH PHẦN THƯỞNG CHO TOP NẠP NGÀY
TOP1_REWARD = 5000
TOP2_REWARD = 2500
TOP3_REWARD = 1000
last_reward_date = ""

# ==================== CẤU HÌNH HỆ THỐNG CƠ BẢN ====================
SUPABASE_URL = "https://npjjarsmvmqvhdnkvtxc.supabase.co" 
SUPABASE_KEY = "sb_publishable_gVXyT92FL0XpsiiEcerYFQ_RXE3n0ke"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

API_ID = 36437338
API_HASH = "18d34c7efc396d277f3db62baa078efc"
BOT_TOKEN = "8654764187:AAGSqHRK59Ood6Z32KktLOpiytlZgWbD24E"

STK_MSB = "96886693002613"
ADMIN_ID = 7816353760 

logging.basicConfig(level=logging.INFO)
bot = TelegramClient(StringSession(), API_ID, API_HASH)

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

# Thêm biến Cache Global để chống rate limit Supabase
cached_categories = []
last_cache_time = 0

# ==================== HỆ THỐNG QUẢN LÝ EMOJI ĐỘNG ====================
DEFAULT_EMOJIS = {
    "bot": "🤖", "user": "👤", "money": "💰", "vip": "🎖",
    "cart": "🛒", "bank": "🏦", "history": "🕒", "trophy": "🏆",
    "stats": "📊", "handshake": "🤝", "support": "💬", "admin": "👑",
    "check": "✅", "cross": "❌", "box": "📦", "gift": "🎁",
    "warning": "⚠️", "speaker": "📢", "game": "🎮", "key": "🔑",
    "medal1": "🥇", "medal2": "🥈", "medal3": "🥉"
}
EMOJI_CACHE = DEFAULT_EMOJIS.copy()

async def init_emojis():
    global EMOJI_CACHE
    try:
        for k, v in DEFAULT_EMOJIS.items():
            db_val = await db_get_setting(f"EMO_{k}", v)
            EMOJI_CACHE[k] = db_val
    except Exception as e:
        logging.error(f"Lỗi tải Emoji: {e}")

# ==================== HELPER FUNCTIONS & DATABASE ====================
async def db_get_user(uid):
    try:
        res = await asyncio.to_thread(lambda: supabase.table("users").select("*").eq("user_id", uid).execute())
        if not res.data:
            await asyncio.to_thread(lambda: supabase.table("users").insert({"user_id": uid, "balance": 0, "role": "user", "ctv_balance": 0}).execute())
            return {"user_id": uid, "balance": 0, "role": "user", "ctv_balance": 0}
        
        user_data = res.data[0]
        if 'ctv_balance' not in user_data:
            user_data['ctv_balance'] = 0
        return user_data
    except Exception as e:
        logging.error(f"Lỗi db_get_user: {e}")
        return {"user_id": uid, "balance": 0, "role": "user", "ctv_balance": 0}

def sync_db_get_user(uid):
    try:
        res = supabase.table("users").select("*").eq("user_id", uid).execute()
        if not res.data:
            supabase.table("users").insert({"user_id": uid, "balance": 0, "role": "user", "ctv_balance": 0}).execute()
            return {"user_id": uid, "balance": 0, "role": "user", "ctv_balance": 0}
        
        user_data = res.data[0]
        if 'ctv_balance' not in user_data:
            user_data['ctv_balance'] = 0
        return user_data
    except Exception as e:
        logging.error(f"Lỗi sync_db_get_user: {e}")
        return {"user_id": uid, "balance": 0, "role": "user", "ctv_balance": 0}

async def db_get_setting(key, default_value):
    try:
        res = await asyncio.to_thread(lambda: supabase.table("settings").select("value").eq("key", key).execute())
        if not res.data:
            await asyncio.to_thread(lambda: supabase.table("settings").insert({"key": key, "value": str(default_value)}).execute())
            return str(default_value)
        return res.data[0]['value']
    except Exception as e:
        logging.error(f"Lỗi db_get_setting: {e}")
        return str(default_value)

def sync_db_get_setting(key, default_value):
    try:
        res = supabase.table("settings").select("value").eq("key", key).execute()
        if not res.data:
            supabase.table("settings").insert({"key": key, "value": str(default_value)}).execute()
            return str(default_value)
        return res.data[0]['value']
    except Exception as e:
        logging.error(f"Lỗi sync_db_get_setting: {e}")
        return str(default_value)

async def db_set_setting(key, value):
    try:
        res = await asyncio.to_thread(lambda: supabase.table("settings").select("value").eq("key", key).execute())
        if not res.data:
            await asyncio.to_thread(lambda: supabase.table("settings").insert({"key": key, "value": str(value)}).execute())
        else:
            await asyncio.to_thread(lambda: supabase.table("settings").update({"value": str(value)}).eq("key", key).execute())
    except Exception as e:
        logging.error(f"Lỗi db_set_setting: {e}")

def sync_db_set_setting(key, value):
    try:
        res = supabase.table("settings").select("value").eq("key", key).execute()
        if not res.data:
            supabase.table("settings").insert({"key": key, "value": str(value)}).execute()
        else:
            supabase.table("settings").update({"value": str(value)}).eq("key", key).execute()
    except Exception as e:
        logging.error(f"Lỗi sync_db_set_setting: {e}")

async def get_user_level_and_discount(uid):
    try:
        res = await asyncio.to_thread(lambda: supabase.table("history").select("amount").eq("user_id", uid).eq("action", "Nạp tiền").execute())
        total_dep = sum([r['amount'] for r in res.data]) if getattr(res, 'data', None) else 0
        
        if total_dep >= 10000000:
            return 3, 0.10, total_dep
        elif total_dep >= 5000000:
            return 2, 0.07, total_dep
        elif total_dep >= 2000000:
            return 1, 0.05, total_dep
        else:
            return 0, 0.0, total_dep 
    except Exception as e:
        logging.error(f"Lỗi tính VIP cho user {uid}: {e}")
        return 0, 0.0, 0

# ==================== LOGIC THÔNG BÁO KÊNH & LỊCH SỬ ====================
async def send_channel_notify(text):
    channel_id_str = await db_get_setting("NOTIFY_CHANNEL_ID", "Chưa cài đặt")
    if channel_id_str and channel_id_str != "Chưa cài đặt":
        try:
            await bot.send_message(int(channel_id_str), text)
        except Exception as e:
            logging.error(f"Lỗi gửi thông báo kênh: {e}")

def sync_send_channel_notify(text):
    channel_id_str = sync_db_get_setting("NOTIFY_CHANNEL_ID", "Chưa cài đặt")
    if channel_id_str and channel_id_str != "Chưa cài đặt":
        try:
            asyncio.run_coroutine_threadsafe(bot.send_message(int(channel_id_str), text), loop)
        except Exception as e:
            logging.error(f"Lỗi gửi thông báo kênh sync: {e}")

async def db_add_history(uid, action, game_name, qty, amount, codes_list=""):
    try:
        now_str = datetime.now(timezone.utc).isoformat()
        await asyncio.to_thread(lambda: supabase.table("history").insert({
            "user_id": uid, "action": action, "game_name": game_name, 
            "qty": qty, "amount": amount, "codes_list": codes_list, "created_at": now_str
        }).execute())
    except Exception as e:
        logging.error(f"Lỗi lưu lịch sử: {e}")

def sync_db_add_history(uid, action, game_name, qty, amount, codes_list=""):
    try:
        now_str = datetime.now(timezone.utc).isoformat()
        supabase.table("history").insert({
            "user_id": uid, "action": action, "game_name": game_name, 
            "qty": qty, "amount": amount, "codes_list": codes_list, "created_at": now_str
        }).execute()
    except Exception as e:
        logging.error(f"Lỗi lưu lịch sử sync: {e}")

async def auto_clean_history():
    while True:
        try:
            yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
            await asyncio.to_thread(lambda: supabase.table("history").delete().lt("created_at", yesterday).execute())
        except Exception as e:
            logging.error(f"Lỗi tự động xóa lịch sử cũ: {e}")
        await asyncio.sleep(3600)

async def auto_daily_reward():
    global last_reward_date
    while True:
        try:
            now = datetime.now(VN_TZ)
            current_date_str = now.strftime('%Y-%m-%d')
            
            if now.hour == 23 and now.minute == 59 and last_reward_date != current_date_str:
                last_reward_date = current_date_str 
                
                today_start = now.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).isoformat()
                res = await asyncio.to_thread(lambda: supabase.table("history").select("user_id, amount").eq("action", "Nạp tiền").gte("created_at", today_start).execute())
                
                if getattr(res, 'data', None):
                    top_data = {}
                    for r in res.data:
                        uid_str = r['user_id']
                        top_data[uid_str] = top_data.get(uid_str, 0) + r['amount']
                    
                    sorted_top = sorted(top_data.items(), key=lambda x: x[1], reverse=True)[:3]
                    
                    if sorted_top:
                        rewards = [TOP1_REWARD, TOP2_REWARD, TOP3_REWARD]
                        msg_channel = f"{EMOJI_CACHE['gift']} **CHỐT SỔ THƯỞNG TOP NẠP NGÀY HÔM NAY** {EMOJI_CACHE['gift']}\n━━━━━━━━━━━━━━━━━━\n"
                        
                        for i, (uid_str, total_amt) in enumerate(sorted_top):
                            reward_amt = rewards[i]
                            user = await db_get_user(int(uid_str))
                            await asyncio.to_thread(lambda: supabase.table("users").update({"balance": user['balance'] + reward_amt}).eq("user_id", int(uid_str)).execute())
                            
                            try:
                                await bot.send_message(int(uid_str), f"🎉 **CHÚC MỪNG BẠN!**\nBạn đã đạt **Top {i+1} Nạp Ngày**.\nHệ thống đã cộng tự động **{reward_amt:,}đ** tiền thưởng vào tài khoản của bạn!")
                            except: pass
                            
                            msg_channel += f"Top {i+1}: `{uid_str}` - Thưởng: **{reward_amt:,}đ**\n"
                            
                        msg_channel += f"━━━━━━━━━━━━━━━━━━\n{EMOJI_CACHE['check']} *Bảng xếp hạng nạp đã tự động làm mới cho ngày hôm sau!*"
                        await send_channel_notify(msg_channel)
                        
        except Exception as e:
            logging.error(f"Lỗi auto_daily_reward: {e}")
            
        await asyncio.sleep(40) 

# ---> TÍNH NĂNG MỚI: AUTO BROADCAST QUẢNG CÁO MỖI 12 TIẾNG
async def auto_broadcast_ad():
    while True:
        try:
            ad_msg = await db_get_setting("AUTO_AD_MSG", "Chưa cài đặt")
            if ad_msg and ad_msg != "Chưa cài đặt" and ad_msg.strip() != "":
                users_res = await asyncio.to_thread(lambda: supabase.table("users").select("user_id").execute())
                if getattr(users_res, 'data', None):
                    for u in users_res.data:
                        try:
                            await bot.send_message(int(u['user_id']), f"{EMOJI_CACHE['speaker']} **THÔNG TIN TỪ HỆ THỐNG**\n\n{ad_msg}")
                            await asyncio.sleep(0.1)
                        except:
                            pass
        except Exception as e:
            logging.error(f"Lỗi auto spam quảng cáo: {e}")
            
        # Nghỉ 12 tiếng = 43200 giây
        await asyncio.sleep(43200)

# ==================== LOGIC ĐẬP HỘP ĐA DANH MỤC ====================
async def worker_grab_loop(client, phone):
    global cached_categories, last_cache_time
    try:
        if not client.is_connected(): 
            await client.connect()
            
        if not await client.is_user_authorized():
            logging.error(f"Clone {phone} đã chết session (bị đăng xuất).")
            await asyncio.to_thread(lambda: supabase.table("my_clones").update({"status": "dead"}).eq("phone", phone).execute())
            await bot.send_message(ADMIN_ID, f"{EMOJI_CACHE['warning']} **CẢNH BÁO CLONE CHẾT**\nClone `{phone}` đã bị văng session. Vui lòng nạp lại!")
            return

        try:
            cats_res = await asyncio.to_thread(lambda: supabase.table("categories").select("target_bot").execute())
            cats = cats_res.data
            if cats:
                for c in cats:
                    if c.get('target_bot') and c['target_bot'].strip() != "":
                        try:
                            await client.send_message(c['target_bot'], "/start")
                            await asyncio.sleep(1.5) 
                        except Exception as start_err:
                            logging.warning(f"Clone {phone} không thể gửi /start tới {c['target_bot']}: {start_err}")
        except Exception as e:
            logging.error(f"Lỗi khi auto-start bot mục tiêu cho {phone}: {e}")

        @client.on(events.NewMessage())
        @client.on(events.MessageEdited())
        async def handler(ev):
            global cached_categories, last_cache_time
            if not ev.reply_markup: 
                return
            
            chat = await ev.get_chat()
            chat_username = getattr(chat, 'username', '')
            if not chat_username: 
                return

            try:
                current_time = time.time()
                if current_time - last_cache_time > 60 or not cached_categories:
                    cats_data = await asyncio.to_thread(lambda: supabase.table("categories").select("*").execute())
                    if cats_data and getattr(cats_data, 'data', None):
                        cached_categories = cats_data.data
                    last_cache_time = current_time

                if not cached_categories: 
                    return
                
                matched_cat = next((c for c in cached_categories if c.get('target_bot') and c['target_bot'].lower() == chat_username.lower()), None)
                
                if matched_cat:
                    for row in ev.reply_markup.rows:
                        for btn in row.buttons:
                            if btn.text and "đập" in btn.text.lower():
                                await asyncio.sleep(random.uniform(0.1, 0.4))
                                try:
                                    click_res = await ev.click(text=btn.text)
                                    code_found = None
                                    
                                    if click_res and getattr(click_res, 'message', None):
                                        if "là:" in click_res.message:
                                            m_search = re.search(r'là:\s*([A-Z0-9]+)', click_res.message)
                                            if m_search: 
                                                code_found = m_search.group(1)
                                    
                                    if not code_found:
                                        await asyncio.sleep(1.0)
                                        msgs = await client.get_messages(chat.id, limit=2)
                                        for m in msgs:
                                            if m.message and "Mã code của bạn là:" in m.message:
                                                m_match = re.search(r'là:\s*\n?([A-Z0-9]+)', m.message)
                                                if m_match: 
                                                    code_found = m_match.group(1)

                                    if code_found:
                                        check_dup = await asyncio.to_thread(lambda: supabase.table("codes").select("id").eq("code", code_found).execute())
                                        if getattr(check_dup, 'data', None):
                                            return 
                                            
                                        await asyncio.to_thread(lambda: supabase.table("codes").insert({
                                            "code": code_found, 
                                            "status": "available", 
                                            "source_phone": phone,
                                            "category_id": matched_cat['id']
                                        }).execute())
                                        
                                        await bot.send_message(
                                            ADMIN_ID, 
                                            f"🎊 **NHẬN CODE MỚI!** \n{EMOJI_CACHE['game']} Danh mục: **{matched_cat['name']}** \n📱 Clone: `{phone}`\n{EMOJI_CACHE['key']} Code: `{code_found}`"
                                        )
                                        
                                        try:
                                            count_res = await asyncio.to_thread(lambda: supabase.table("codes").select("id", count='exact').eq("category_id", matched_cat['id']).eq("status", "available").execute())
                                            stock = count_res.count if count_res.count is not None else 0
                                            if stock in [20, 40, 60]:
                                                await send_channel_notify(f"🎉 **TIN VUI TỪ KHO GAME**\nKho game **{matched_cat['name']}** vừa đạt mốc **{stock} code**!\nAnh em nhanh tay vào húp nhé!")
                                        except Exception as e_stock:
                                            logging.error(f"Lỗi thông báo mốc code: {e_stock}")
                                        
                                        return
                                except Exception as e:
                                    logging.error(f"Lỗi click đập hộp của {phone}: {e}")
            except Exception as outer_e:
                logging.error(f"Lỗi xử lý tin nhắn đập hộp: {outer_e}")
                
        await client.run_until_disconnected()
    except Exception as e:
        logging.error(f"Worker của clone {phone} đã dừng: {e}")

# ==================== GIAO DIỆN NGƯỜI DÙNG & GIAO DIỆN CTV ====================
async def main_menu_text(user):
    bot_intro = await db_get_setting("BOT_INTRO", "Chào mừng bạn đến với hệ thống bán code tự động!")
    lv, _, total_dep = await get_user_level_and_discount(user['user_id'])
    
    if lv == 0:
        progress_text = f"Đã nạp {total_dep:,}/2,000,000đ (Lên VIP 1)"
    elif lv == 1:
        progress_text = f"Đã nạp {total_dep:,}/5,000,000đ (Lên VIP 2)"
    elif lv == 2:
        progress_text = f"Đã nạp {total_dep:,}/10,000,000đ (Lên VIP 3)"
    else:
        progress_text = f"VIP Tối Đa (Đã nạp {total_dep:,}đ)"
        
    vip_str = f"| {EMOJI_CACHE['vip']} VIP: {lv}\n📈 Tiến độ VIP: {progress_text}" if total_dep >= 0 else ""
    
    return (
        f"{EMOJI_CACHE['bot']} **HỆ THỐNG CỬA HÀNG CODE VIP** {EMOJI_CACHE['bot']}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"{EMOJI_CACHE['user']} ID Của Bạn: `{user['user_id']}` {vip_str}\n"
        f"{EMOJI_CACHE['money']} Số dư: **{user['balance']:,} VNĐ** \n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📝 {bot_intro}"
    )

async def get_main_btns(uid):
    user = await db_get_user(uid)
    support_link = await db_get_setting("SUPPORT_LINK", "https://t.me/admin")
    
    btns = []
    
    if user.get('role') == 'ctv':
        btns.append([TButton.inline(f"💼 KÊNH ĐỐI TÁC (CTV VIP) 💼", b"ctv_dashboard")])
        
    btns.extend([
        [TButton.inline(f"{EMOJI_CACHE['cart']} DANH MỤC GAME", b"list_categories")],
        [TButton.inline(f"{EMOJI_CACHE['bank']} NẠP TIỀN", b"dep_menu"), TButton.inline(f"{EMOJI_CACHE['history']} LỊCH SỬ GIAO DỊCH", b"history")],
        [TButton.inline(f"{EMOJI_CACHE['trophy']} TOP NẠP TRONG NGÀY", b"top_users")], 
        [TButton.inline(f"{EMOJI_CACHE['stats']} THỐNG KÊ HỆ THỐNG", b"global_stats")],
        [TButton.inline(f"{EMOJI_CACHE['handshake']} GIỚI THIỆU (HOA HỒNG 10%)", b"referral_menu")],
        [TButton.url(f"{EMOJI_CACHE['support']} LIÊN HỆ HỖ TRỢ", support_link)], 
    ])
    
    if uid == ADMIN_ID:
        btns.append([TButton.inline(f"{EMOJI_CACHE['admin']} QUẢN TRỊ ADMIN", b"admin_menu")])
    return btns

@bot.on(events.NewMessage(pattern=r"^/start(?: (.*))?$"))
async def start(e):
    uid = e.sender_id
    
    # KIỂM TRA BẢO TRÌ
    status = await db_get_setting("MAINTENANCE_MODE", "OFF")
    if status == "ON" and uid != ADMIN_ID:
        await e.respond(f"{EMOJI_CACHE['warning']} **HỆ THỐNG ĐANG BẢO TRÌ**\nBot đang được nâng cấp, vui lòng quay lại sau nhé!")
        return
        
    payload = e.pattern_match.group(1)
    
    # FIX LỖI ÉP JOIN KÊNH BẰNG TRY-EXCEPT CHẶT HƠN
    channel = await db_get_setting("FORCE_JOIN_CHANNEL", "Chưa cài đặt")
    if channel and channel != "Chưa cài đặt" and channel.strip() != "":
        try:
            channel_entity = int(channel) if str(channel).lstrip('-').isdigit() else channel
            participant = await bot.get_permissions(channel_entity, uid)
            if not participant.is_participant:
                raise ValueError("Not in channel")
        except Exception:
            btns = [[TButton.url(f"{EMOJI_CACHE['speaker']} THAM GIA KÊNH ĐỂ TIẾP TỤC", f"https://t.me/{channel.replace('@', '')}")],
                    [TButton.inline(f"{EMOJI_CACHE['check']} ĐÃ THAM GIA", b"check_join")]]
            await e.respond(f"{EMOJI_CACHE['warning']} **YÊU CẦU BẮT BUỘC**\n\nBạn cần tham gia kênh của chúng tôi để sử dụng Bot. Vui lòng tham gia và bấm nút **ĐÃ THAM GIA** bên dưới.", buttons=btns)
            return

    user = await db_get_user(uid)
    
    if payload and payload.isdigit() and int(payload) != uid:
        referrer_id = int(payload)
        if user.get('referrer_id') is None:
            try:
                await asyncio.to_thread(lambda: supabase.table("users").update({"referrer_id": referrer_id}).eq("user_id", uid).execute())
                user['referrer_id'] = referrer_id
                await bot.send_message(referrer_id, f"🎉 **CÓ NGƯỜI MỚI THAM GIA!**\nThành viên ID `{uid}` vừa đăng ký qua link giới thiệu của bạn. Bạn sẽ nhận **10% hoa hồng** mỗi khi họ nạp tiền!")
            except Exception as ex:
                logging.error(f"Lỗi lưu referrer_id: {ex}")

    text = await main_menu_text(user)
    btns = await get_main_btns(uid)
    await e.respond(text, buttons=btns)

@bot.on(events.CallbackQuery)
async def cb_handler(e):
    uid = e.sender_id
    data = e.data.decode()

    # KIỂM TRA BẢO TRÌ
    maint_status = await db_get_setting("MAINTENANCE_MODE", "OFF")
    if maint_status == "ON" and uid != ADMIN_ID:
        await e.answer("⚠️ Hệ thống đang bảo trì để nâng cấp. Vui lòng quay lại sau!", alert=True)
        return

    if data == "back":
        await e.answer() 
        user = await db_get_user(uid)
        text = await main_menu_text(user)
        btns = await get_main_btns(uid)
        await e.edit(text, buttons=btns)

    elif data == "check_join":
        channel = await db_get_setting("FORCE_JOIN_CHANNEL", "Chưa cài đặt")
        if channel and channel != "Chưa cài đặt" and channel.strip() != "":
            try:
                channel_entity = int(channel) if str(channel).lstrip('-').isdigit() else channel
                participant = await bot.get_permissions(channel_entity, uid)
                if not participant.is_participant:
                    await e.answer("❌ Bạn chưa tham gia kênh! Vui lòng tham gia để sử dụng bot.", alert=True)
                    return
            except Exception:
                await e.answer("❌ Bạn chưa tham gia kênh! Vui lòng tham gia để sử dụng bot.", alert=True)
                return
                
        await e.answer("✅ Xác nhận thành công!", alert=True)
        user = await db_get_user(uid)
        text = await main_menu_text(user)
        btns = await get_main_btns(uid)
        await e.edit(text, buttons=btns)

    # ==================== KHU VỰC DÀNH RIÊNG CHO CTV ====================
    elif data == "ctv_dashboard":
        await e.answer()
        user = await db_get_user(uid)
        if user.get('role') != 'ctv':
            await e.answer("❌ Bạn không có quyền truy cập!", alert=True)
            return
            
        txt = (f"💼 **TRUNG TÂM ĐỐI TÁC (CỘNG TÁC VIÊN)** 💼\n"
               f"━━━━━━━━━━━━━━━━━━\n"
               f"{EMOJI_CACHE['user']} **CTV:** `{uid}`\n"
               f"{EMOJI_CACHE['money']} **Số dư ví CTV (Hoa hồng):** **{user.get('ctv_balance', 0):,} VNĐ**\n"
               f"*(Hệ thống sẽ thu phí admin 10% doanh thu mỗi khi có đơn thành công)*\n"
               f"━━━━━━━━━━━━━━━━━━\n"
               f"Vui lòng chọn chức năng quản lý bên dưới:")
               
        btns = [
            [TButton.inline("➕ THÊM DANH MỤC (SẢN PHẨM) MỚI", b"ctv_add_cat")],
            [TButton.inline(f"{EMOJI_CACHE['box']} UP CODE LÊN SẢN PHẨM CỦA BẠN", b"ctv_add_codes")],
            [TButton.inline("📜 LỊCH SỬ ĐƠN HÀNG ĐÃ BÁN", b"ctv_my_history")],
            [TButton.inline("💳 RÚT DOANH THU VỀ BANK", b"ctv_withdraw")],
            [TButton.inline("🔙 QUAY LẠI TRANG CHỦ", b"back")]
        ]
        await e.edit(txt, buttons=btns)

    elif data == "ctv_my_history":
        await e.answer()
        user = await db_get_user(uid)
        if user.get('role') != 'ctv': return
        
        res = await asyncio.to_thread(lambda: supabase.table("ctv_history").select("*").eq("ctv_id", uid).order("created_at", desc=True).limit(15).execute())
        if not getattr(res, 'data', None):
            await e.edit(f"{EMOJI_CACHE['cross']} Bạn chưa có đơn hàng nào được bán ra.", buttons=[[TButton.inline("🔙 VỀ MENU CTV", b"ctv_dashboard")]])
            return
            
        txt = f"📜 **LỊCH SỬ BÁN HÀNG CỦA BẠN**\n━━━━━━━━━━━━━━━━━━\n"
        for h in res.data:
            dt = datetime.fromisoformat(h['created_at'].replace('Z', '+00:00'))
            time_str = dt.astimezone(VN_TZ).strftime('%H:%M %d/%m')
            txt += f"🔹 `{time_str}` | Đã bán {h['qty']} code {h['category_name']}\n"
            txt += f"   {EMOJI_CACHE['user']} ID Khách mua: `{h['buyer_id']}`\n"
            txt += f"   {EMOJI_CACHE['money']} Hoa hồng nhận: **+{h['revenue']:,}đ** (Phí: -{h['admin_fee']:,}đ)\n"
            
        await e.edit(txt, buttons=[[TButton.inline("🔙 VỀ MENU CTV", b"ctv_dashboard")]])

    elif data == "ctv_add_cat":
        await e.answer()
        user = await db_get_user(uid)
        if user.get('role') != 'ctv': return
        await e.delete()
        async with bot.conversation(uid) as conv:
            try:
                await conv.send_message(f"🌟 **[CTV] TẠO SẢN PHẨM MỚI**\n👉 Nhập Tên Game/Sản phẩm (VD: Ok vip random 8-88k):")
                name = (await conv.get_response()).text.strip()
                
                await conv.send_message(f"{EMOJI_CACHE['money']} Nhập Giá bán ra cho khách hàng (Ghi số, VD: 15000):")
                price = int((await conv.get_response()).text.strip())
                
                await conv.send_message("📝 Nhập Mô tả sản phẩm để khách đọc:")
                desc = (await conv.get_response()).text.strip()
                
                await asyncio.to_thread(lambda: supabase.table("categories").insert({"name": name, "price": price, "description": desc, "owner_id": uid, "target_bot": ""}).execute())
                await conv.send_message(f"{EMOJI_CACHE['check']} Đã tạo sản phẩm thành công: **{name}**\nBây giờ bạn có thể UP CODE vào mục này!", buttons=[[TButton.inline("🔙 VỀ MENU CTV", b"ctv_dashboard")]])
            except ValueError:
                await conv.send_message(f"{EMOJI_CACHE['cross']} Lỗi: Giá bán phải là số!", buttons=[[TButton.inline("🔙 VỀ MENU CTV", b"ctv_dashboard")]])
            except Exception as ex:
                logging.error(f"Lỗi CTV tạo danh mục: {ex}")
                await conv.send_message(f"{EMOJI_CACHE['cross']} Có lỗi xảy ra!", buttons=[[TButton.inline("🔙 VỀ MENU CTV", b"ctv_dashboard")]])

    elif data == "ctv_add_codes":
        await e.answer()
        user = await db_get_user(uid)
        if user.get('role') != 'ctv': return
        
        cats_res = await asyncio.to_thread(lambda: supabase.table("categories").select("*").eq("owner_id", uid).execute())
        cats = cats_res.data
        if not cats:
            await e.edit(f"{EMOJI_CACHE['cross']} Bạn chưa tạo sản phẩm nào để up code. Vui lòng tạo Sản Phẩm trước!", buttons=[[TButton.inline("🔙 VỀ MENU CTV", b"ctv_dashboard")]])
            return
            
        txt = f"{EMOJI_CACHE['box']} **SẢN PHẨM CỦA BẠN ĐANG CÓ:**\n"
        for c in cats:
            txt += f"🔸 ID: `{c['id']}` - Tên: **{c['name']}**\n"
            
        await e.delete()
        async with bot.conversation(uid) as conv:
            try:
                await conv.send_message(txt + "\n👉 Nhập ID Sản Phẩm bạn muốn UP CODE vào:")
                cat_id = int((await conv.get_response()).text.strip())
                
                valid_cat = next((c for c in cats if c['id'] == cat_id), None)
                if not valid_cat:
                    await conv.send_message(f"{EMOJI_CACHE['cross']} Bạn không sở hữu ID sản phẩm này!", buttons=[[TButton.inline("🔙 VỀ MENU CTV", b"ctv_dashboard")]])
                    return
                
                await conv.send_message("👉 Gửi danh sách code (Mỗi code nằm trên 1 dòng riêng biệt):")
                codes_msg = await conv.get_response()
                raw_codes = codes_msg.text.strip().split('\n')
                
                insert_data = []
                for c in raw_codes:
                    if c.strip():
                        insert_data.append({"code": c.strip(), "status": "available", "source_phone": f"CTV_{uid}", "category_id": cat_id})
                
                if insert_data:
                    await asyncio.to_thread(lambda: supabase.table("codes").insert(insert_data).execute())
                    await conv.send_message(f"{EMOJI_CACHE['check']} Đã tải lên thành công {len(insert_data)} code cho sản phẩm **{valid_cat['name']}**!", buttons=[[TButton.inline("🔙 VỀ MENU CTV", b"ctv_dashboard")]])
                else:
                    await conv.send_message(f"{EMOJI_CACHE['cross']} Bạn chưa nhập code nào.", buttons=[[TButton.inline("🔙 VỀ MENU CTV", b"ctv_dashboard")]])
            except ValueError:
                await conv.send_message(f"{EMOJI_CACHE['cross']} Lỗi: ID phải là số!", buttons=[[TButton.inline("🔙 VỀ MENU CTV", b"ctv_dashboard")]])

    elif data == "ctv_withdraw":
        await e.answer("Đang tải dữ liệu...", cache_time=0)
        user = await db_get_user(uid)
        if user.get('role') != 'ctv': return
        
        current_ctv_balance = user.get('ctv_balance', 0)
        
        if current_ctv_balance < 50000:
            await bot.send_message(uid, f"{EMOJI_CACHE['cross']} Số dư ví CTV tối thiểu để rút là 50,000 VNĐ!")
            return
            
        await e.delete()
        async with bot.conversation(uid) as conv:
            try:
                await conv.send_message(f"💳 **RÚT TIỀN HOA HỒNG CTV**\nSố dư khả dụng: {current_ctv_balance:,}đ\n👉 Nhập SỐ TIỀN muốn rút:")
                amount = int((await conv.get_response()).text.strip())
                
                if amount < 50000 or amount > current_ctv_balance:
                    await conv.send_message(f"{EMOJI_CACHE['cross']} Số tiền không hợp lệ hoặc lớn hơn số dư ví CTV!", buttons=[[TButton.inline("🔙 VỀ MENU CTV", b"ctv_dashboard")]])
                    return
                    
                await conv.send_message(f"{EMOJI_CACHE['bank']} Nhập thông tin Nhận Tiền (Tên Ngân Hàng - STK - Tên Chủ Tài Khoản):")
                bank_info = (await conv.get_response()).text.strip()
                
                await asyncio.to_thread(lambda: supabase.table("users").update({"ctv_balance": current_ctv_balance - amount}).eq("user_id", uid).execute())
                
                insert_res = await asyncio.to_thread(lambda: supabase.table("withdraw_requests").insert({
                    "user_id": uid, "amount": amount, "bank_info": bank_info, "status": "pending"
                }).execute())
                
                if not insert_res.data:
                    raise Exception("Không thể tạo lệnh rút trong CSDL.")
                    
                req_id = insert_res.data[0]['id']
                
                admin_txt = (f"🔔 **CÓ YÊU CẦU RÚT TIỀN TỪ CTV** 🔔\n"
                             f"{EMOJI_CACHE['user']} CTV ID: `{uid}`\n"
                             f"{EMOJI_CACHE['money']} Số tiền rút: **{amount:,}đ**\n"
                             f"{EMOJI_CACHE['bank']} Bank: `{bank_info}`\n"
                             f"{EMOJI_CACHE['warning']} Hãy chuyển khoản cho họ rồi ấn DUYỆT nhé!")
                admin_btns = [
                    [TButton.inline(f"{EMOJI_CACHE['check']} ĐÃ CHUYỂN & DUYỆT ĐƠN", f"approve_wd_{req_id}_{uid}_{amount}")],
                    [TButton.inline(f"{EMOJI_CACHE['cross']} TỪ CHỐI (HOÀN TIỀN LẠI)", f"reject_wd_{req_id}_{uid}_{amount}")]
                ]
                await bot.send_message(ADMIN_ID, admin_txt, buttons=admin_btns)
                
                await conv.send_message(f"{EMOJI_CACHE['check']} Đã gửi yêu cầu rút tiền thành công! Admin sẽ xử lý sớm nhất.", buttons=[[TButton.inline("🔙 VỀ MENU CTV", b"ctv_dashboard")]])
            except ValueError:
                await conv.send_message(f"{EMOJI_CACHE['cross']} Lỗi: Số tiền phải là số nguyên!")
            except Exception as ex:
                logging.error(f"Lỗi rút tiền CTV: {ex}")
                await conv.send_message(f"{EMOJI_CACHE['cross']} Quá thời gian chờ hoặc có lỗi kết nối CSDL.", buttons=[[TButton.inline("🔙 VỀ MENU CTV", b"ctv_dashboard")]])

    # ==================== ADMIN: XỬ LÝ CTV ====================
    elif data == "admin_ctv":
        await e.answer()
        if uid != ADMIN_ID: return
        btns = [
            [TButton.inline("➕ CẤP / HỦY QUYỀN CTV", b"admin_ctv_role")],
            [TButton.inline("📜 XEM LỊCH SỬ BÁN CODE CỦA CTV", b"admin_ctv_history")],
            [TButton.inline(f"{EMOJI_CACHE['money']} CỘNG / TRỪ VÍ CTV", b"admin_money_ctv")],
            [TButton.inline("🔙 ADMIN", b"admin_menu")]
        ]
        await e.edit(f"{EMOJI_CACHE['handshake']} **QUẢN LÝ ĐỐI TÁC (CTV)**\nVui lòng chọn chức năng:", buttons=btns)

    elif data == "admin_ctv_role":
        await e.answer()
        if uid != ADMIN_ID: return
        await e.delete()
        async with bot.conversation(uid) as conv:
            try:
                await conv.send_message(f"{EMOJI_CACHE['handshake']} **CẤP/HỦY QUYỀN CTV**\n👉 Nhập ID của người bạn muốn CẤP (Hoặc HỦY) quyền:")
                target_id = int((await conv.get_response()).text.strip())
                target_user = await db_get_user(target_id)
                
                if target_user.get('role') == 'ctv':
                    await asyncio.to_thread(lambda: supabase.table("users").update({"role": "user"}).eq("user_id", target_id).execute())
                    await bot.send_message(target_id, f"{EMOJI_CACHE['cross']} Quyền Cộng Tác Viên (CTV) của bạn đã bị Admin thu hồi.")
                    await conv.send_message(f"{EMOJI_CACHE['check']} Đã THU HỒI quyền CTV của tài khoản `{target_id}`", buttons=[[TButton.inline("🔙 QUẢN LÝ CTV", b"admin_ctv")]])
                else:
                    await asyncio.to_thread(lambda: supabase.table("users").update({"role": "ctv"}).eq("user_id", target_id).execute())
                    await bot.send_message(target_id, f"🎉 **CHÚC MỪNG!**\nBạn đã được Admin cấp quyền **ĐỐI TÁC (CỘNG TÁC VIÊN)**.\nHãy vào Menu chính để truy cập Kênh Đối Tác nhé!")
                    await conv.send_message(f"{EMOJI_CACHE['check']} Đã CẤP quyền CTV thành công cho tài khoản `{target_id}`", buttons=[[TButton.inline("🔙 QUẢN LÝ CTV", b"admin_ctv")]])
            except ValueError:
                await conv.send_message(f"{EMOJI_CACHE['cross']} ID phải là số!", buttons=[[TButton.inline("🔙 QUẢN LÝ CTV", b"admin_ctv")]])

    elif data == "admin_money_ctv":
        await e.answer()
        if uid != ADMIN_ID: return
        await e.delete()
        async with bot.conversation(uid) as conv:
            try:
                await conv.send_message(f"{EMOJI_CACHE['user']} Nhập ID của CTV cần cộng/trừ tiền:")
                tid = int((await conv.get_response()).text.strip())
                
                target_user = await db_get_user(tid)
                if target_user.get('role') != 'ctv':
                    await conv.send_message(f"{EMOJI_CACHE['cross']} Lỗi: Người dùng này không phải là Cộng Tác Viên!", buttons=[[TButton.inline("🔙 QUẢN LÝ CTV", b"admin_ctv")]])
                    return
                
                await conv.send_message(f"{EMOJI_CACHE['money']} Nhập số tiền VÍ CTV (Cộng thêm thì ghi 50000, Trừ đi thì ghi -50000):")
                amt = int((await conv.get_response()).text.strip())
                
                new_balance = int(target_user.get('ctv_balance', 0)) + amt
                if new_balance < 0: new_balance = 0
                
                await asyncio.to_thread(lambda: supabase.table("users").update({"ctv_balance": new_balance}).eq("user_id", tid).execute())
                await conv.send_message(f"{EMOJI_CACHE['check']} Thành công! Số dư VÍ CTV mới của `{tid}` là: **{new_balance:,}đ**", buttons=[[TButton.inline("🔙 QUẢN LÝ CTV", b"admin_ctv")]])
            except ValueError:
                await conv.send_message(f"{EMOJI_CACHE['cross']} ID và Số tiền phải là chữ số!", buttons=[[TButton.inline("🔙 QUẢN LÝ CTV", b"admin_ctv")]])
            except Exception as ex:
                logging.error(f"Lỗi admin_money_ctv: {ex}")
                await conv.send_message(f"{EMOJI_CACHE['cross']} Lỗi cơ sở dữ liệu!", buttons=[[TButton.inline("🔙 QUẢN LÝ CTV", b"admin_ctv")]])

    elif data == "admin_ctv_history":
        await e.answer()
        if uid != ADMIN_ID: return
        await e.delete()
        async with bot.conversation(uid) as conv:
            try:
                await conv.send_message("📜 Nhập ID của CTV cần xem lịch sử bán hàng:")
                ctv_id = int((await conv.get_response()).text.strip())
                
                res = await asyncio.to_thread(lambda: supabase.table("ctv_history").select("*").eq("ctv_id", ctv_id).order("created_at", desc=True).limit(10).execute())
                if not getattr(res, 'data', None):
                    await conv.send_message(f"{EMOJI_CACHE['cross']} CTV `{ctv_id}` chưa bán được đơn hàng nào.", buttons=[[TButton.inline("🔙 QUẢN LÝ CTV", b"admin_ctv")]])
                    return
                
                txt = f"📜 **LỊCH SỬ BÁN CỦA CTV: `{ctv_id}`**\n━━━━━━━━━━━━━━━━━━\n"
                for h in res.data:
                    dt = datetime.fromisoformat(h['created_at'].replace('Z', '+00:00'))
                    time_str = dt.astimezone(VN_TZ).strftime('%H:%M %d/%m')
                    txt += f"🔹 `{time_str}` | Bán {h['qty']} {h['category_name']}\n"
                    txt += f"   {EMOJI_CACHE['user']} Khách mua: `{h['buyer_id']}`\n"
                    txt += f"   {EMOJI_CACHE['money']} Hoa hồng nhận: **+{h['revenue']:,}đ** (Phí: -{h['admin_fee']:,}đ)\n"
                
                await conv.send_message(txt, buttons=[[TButton.inline("🔙 QUẢN LÝ CTV", b"admin_ctv")]])
            except ValueError:
                await conv.send_message(f"{EMOJI_CACHE['cross']} ID phải là số!", buttons=[[TButton.inline("🔙 QUẢN LÝ CTV", b"admin_ctv")]])
            except Exception as ex:
                logging.error(f"Lỗi check lịch sử CTV: {ex}")
                await conv.send_message(f"{EMOJI_CACHE['cross']} Lỗi truy xuất cơ sở dữ liệu!", buttons=[[TButton.inline("🔙 QUẢN LÝ CTV", b"admin_ctv")]])

    elif data.startswith("approve_wd_"):
        await e.answer()
        if uid != ADMIN_ID: return
        parts = data.split("_")
        req_id, target_id, amount = int(parts[2]), int(parts[3]), int(parts[4])
        
        await asyncio.to_thread(lambda: supabase.table("withdraw_requests").update({"status": "approved"}).eq("id", req_id).execute())
        await e.edit(f"{EMOJI_CACHE['check']} Đã đánh dấu duyệt thành công lệnh rút **{amount:,}đ** của CTV `{target_id}`.")
        try:
            await bot.send_message(target_id, f"{EMOJI_CACHE['check']} **THÔNG BÁO RÚT TIỀN**\nYêu cầu rút **{amount:,}đ** của bạn đã được Admin duyệt và chuyển khoản thành công!")
        except: pass

    elif data.startswith("reject_wd_"):
        await e.answer()
        if uid != ADMIN_ID: return
        parts = data.split("_")
        req_id, target_id, amount = int(parts[2]), int(parts[3]), int(parts[4])
        
        ctv_user = await db_get_user(target_id)
        new_ctv_balance = ctv_user.get('ctv_balance', 0) + amount
        await asyncio.to_thread(lambda: supabase.table("users").update({"ctv_balance": new_ctv_balance}).eq("user_id", target_id).execute())
        await asyncio.to_thread(lambda: supabase.table("withdraw_requests").update({"status": "rejected"}).eq("id", req_id).execute())
        
        await e.edit(f"{EMOJI_CACHE['cross']} Đã TỪ CHỐI lệnh rút {amount:,}đ của CTV `{target_id}`. Tiền đã được hoàn lại vào ví họ.")
        try:
            await bot.send_message(target_id, f"{EMOJI_CACHE['cross']} **THÔNG BÁO RÚT TIỀN**\nYêu cầu rút **{amount:,}đ** của bạn bị từ chối. Số tiền đã được hoàn lại vào ví.")
        except: pass

    elif data == "referral_menu":
        await e.answer()
        bot_info = await bot.get_me()
        ref_link = f"https://t.me/{bot_info.username}?start={uid}"
        txt = (f"{EMOJI_CACHE['handshake']} **CHƯƠNG TRÌNH HOA HỒNG GIỚI THIỆU** {EMOJI_CACHE['handshake']}\n"
               f"━━━━━━━━━━━━━━━━━━\n"
               f"{EMOJI_CACHE['gift']} **Nhận ngay 10%** giá trị mỗi lần nạp của người mà bạn giới thiệu (Không giới hạn số lần nạp).\n\n"
               f"🔗 **Link giới thiệu của bạn:**\n`{ref_link}`\n\n"
               f"*(Hãy copy link trên và gửi cho bạn bè để bắt đầu kiếm tiền thụ động nhé!)*")
        await e.edit(txt, buttons=[[TButton.inline("🔙 QUAY LẠI TRANG CHỦ", b"back")]])

    elif data == "global_stats":
        await e.answer()
        try:
            u_res = await asyncio.to_thread(lambda: supabase.table("users").select("user_id", count='exact').limit(1).execute())
            total_users = u_res.count if u_res.count else 0
            
            total_dep = int(await db_get_setting("TOTAL_DEPOSIT", "0"))
            total_sold = int(await db_get_setting("TOTAL_CODES_SOLD", "0"))
            
            txt = (f"{EMOJI_CACHE['stats']} **BẢNG THỐNG KÊ HỆ THỐNG** {EMOJI_CACHE['stats']}\n"
                   f"━━━━━━━━━━━━━━━━━━\n"
                   f"👥 **Tổng số thành viên:** `{total_users:,}` người\n"
                   f"💵 **Tổng tiền đã nạp:** `{total_dep:,} VNĐ`\n"
                   f"{EMOJI_CACHE['box']} **Tổng lượt mua code:** `{total_sold:,}` mã\n"
                   f"━━━━━━━━━━━━━━━━━━\n"
                   f"{EMOJI_CACHE['check']} *Hệ thống uy tín, tự động và minh bạch 24/7!*")
            await e.edit(txt, buttons=[[TButton.inline("🔙 QUAY LẠI TRANG CHỦ", b"back")]])
        except Exception as ex:
            logging.error(f"Lỗi thống kê: {ex}")
            await e.edit(f"{EMOJI_CACHE['cross']} Đang tải dữ liệu thống kê, vui lòng thử lại sau.", buttons=[[TButton.inline("🔙 QUAY LẠI", b"back")]])

    elif data == "top_users":
        await e.answer()
        try:
            today_start = datetime.now(VN_TZ).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).isoformat()
            res = await asyncio.to_thread(lambda: supabase.table("history").select("user_id, amount").eq("action", "Nạp tiền").gte("created_at", today_start).execute())
            
            top_data = {}
            if getattr(res, 'data', None):
                for r in res.data:
                    uid_str = r['user_id']
                    top_data[uid_str] = top_data.get(uid_str, 0) + r['amount']
            
            sorted_top = sorted(top_data.items(), key=lambda x: x[1], reverse=True)[:10]
            
            if not sorted_top:
                await e.edit(f"{EMOJI_CACHE['trophy']} Hôm nay chưa có đại gia nào nạp tiền.", buttons=[[TButton.inline("🔙 QUAY LẠI", b"back")]])
                return
            
            txt = f"{EMOJI_CACHE['trophy']} **BẢNG XẾP HẠNG TOP NẠP HÔM NAY** {EMOJI_CACHE['trophy']}\n━━━━━━━━━━━━━━━━━━\n"
            medals = [EMOJI_CACHE['medal1'], EMOJI_CACHE['medal2'], EMOJI_CACHE['medal3'], "🏅", "🏅", "🏅", "🏅", "🏅", "🏅", "🏅"]
            for i, (user_id, total_amt) in enumerate(sorted_top):
                txt += f"{medals[i]} ID: `{user_id}` - Tổng nạp: **{total_amt:,}đ**\n"
            txt += "━━━━━━━━━━━━━━━━━━\n🕒 Cập nhật lúc: " + datetime.now(VN_TZ).strftime('%H:%M %d/%m/%Y')
            txt += f"\n{EMOJI_CACHE['gift']} 3 Top đầu sẽ được hệ thống cộng thưởng tự động vào cuối ngày!*"
            
            await e.edit(txt, buttons=[[TButton.inline("🔙 QUAY LẠI TRANG CHỦ", b"back")]])
        except Exception as ex:
            logging.error(f"Lỗi xem TOP: {ex}")
            await e.edit(f"{EMOJI_CACHE['cross']} Lỗi tải bảng xếp hạng.", buttons=[[TButton.inline("🔙 QUAY LẠI", b"back")]])

    elif data == "admin_menu":
        await e.answer() 
        if uid != ADMIN_ID: 
            return
        btns = [
            [TButton.inline(f"{EMOJI_CACHE['handshake']} QUẢN LÝ ĐỐI TÁC (CTV)", b"admin_ctv")],
            [TButton.inline("📂 QUẢN LÝ DANH MỤC", b"admin_cats"), TButton.inline("📱 QUẢN LÝ CLONE", b"admin_clones")],
            [TButton.inline("⚙️ CÀI ĐẶT CHUNG", b"admin_settings"), TButton.inline(f"{EMOJI_CACHE['money']} CỘNG/TRỪ TIỀN", b"admin_money")],
            [TButton.inline("🕵️ CHECK LỊCH SỬ GD", b"admin_check_history")],
            [TButton.inline(f"{EMOJI_CACHE['trophy']} BẮN THÔNG BÁO TOP NẠP", b"admin_notify_top")], 
            [TButton.inline(f"{EMOJI_CACHE['speaker']} BẮN THÔNG BÁO CHO USER", b"admin_broadcast")],
            [TButton.inline("✨ QUẢN LÝ EMOJI PREMIUM", b"admin_emoji")], 
            [TButton.inline("🔙 TRANG CHỦ", b"back")]
        ]
        await e.edit(f"👨‍💻 **BẢNG ĐIỀU KHIỂN ADMIN** ", buttons=btns)

    # ---> TÍNH NĂNG MỚI: QUẢN LÝ EMOJI BẰNG CƠ SỞ DỮ LIỆU
    elif data == "admin_emoji":
        await e.answer()
        if uid != ADMIN_ID: return
        await e.delete()
        async with bot.conversation(uid) as conv:
            try:
                msg = f"✨ **HỆ THỐNG QUẢN LÝ EMOJI ĐỘNG** ✨\n\nBạn có thể đổi các emoji dưới đây bằng cách copy emoji mới (hoặc Premium Emoji) và dán vào. Các nút bấm cũng sẽ tự động thay đổi.\n\n"
                msg += "**Các mã hiện có:**\n" + ", ".join([f"`{k}`" for k in DEFAULT_EMOJIS.keys()])
                msg += "\n\n👉 **Nhập TÊN MÃ muốn đổi (Ví dụ: bot, user, money...):**"
                await conv.send_message(msg)
                
                key_to_edit = (await conv.get_response()).text.strip().lower()
                
                if key_to_edit not in DEFAULT_EMOJIS:
                    await conv.send_message(f"{EMOJI_CACHE['cross']} Mã không hợp lệ! Vui lòng làm lại.", buttons=[[TButton.inline("🔙 ADMIN", b"admin_menu")]])
                    return
                
                await conv.send_message(f"👉 **Gửi EMOJI MỚI cho mã `{key_to_edit}`:**\n*(Lưu ý: Nếu gửi emoji thường, nút bấm cũng sẽ đổi. Telegram không hỗ trợ Emoji Premium trong nút bấm).*")
                new_em = (await conv.get_response()).text.strip()
                
                await db_set_setting(f"EMO_{key_to_edit}", new_em)
                EMOJI_CACHE[key_to_edit] = new_em # Cập nhật trực tiếp trên cache
                
                await conv.send_message(f"{EMOJI_CACHE['check']} Đã cập nhật thành công Emoji mới cho mã `{key_to_edit}`!", buttons=[[TButton.inline("🔙 ADMIN", b"admin_menu")]])
            except Exception as ex:
                logging.error(f"Lỗi admin emoji: {ex}")
                await conv.send_message(f"{EMOJI_CACHE['cross']} Đã quá thời gian hoặc lỗi!", buttons=[[TButton.inline("🔙 ADMIN", b"admin_menu")]])

    elif data == "admin_broadcast":
        await e.answer()
        if uid != ADMIN_ID: return
        await e.delete()
        async with bot.conversation(uid) as conv:
            try:
                await conv.send_message(f"{EMOJI_CACHE['speaker']} Nhập nội dung thông báo bạn muốn gửi đến TOÀN BỘ THÀNH VIÊN:")
                msg = (await conv.get_response()).text.strip()
                
                users_res = await asyncio.to_thread(lambda: supabase.table("users").select("user_id").execute())
                if getattr(users_res, 'data', None):
                    success = 0
                    await conv.send_message("⏳ Đang tiến hành gửi, vui lòng đợi hệ thống chạy...")
                    for u in users_res.data:
                        try:
                            await bot.send_message(int(u['user_id']), f"{EMOJI_CACHE['speaker']} **THÔNG BÁO TỪ ADMIN**\n\n{msg}")
                            success += 1
                            await asyncio.sleep(0.1) 
                        except Exception: 
                            pass
                    await conv.send_message(f"{EMOJI_CACHE['check']} Đã gửi thông báo thành công đến {success} người dùng!", buttons=[[TButton.inline("🔙 QUAY LẠI ADMIN", b"admin_menu")]])
                else:
                    await conv.send_message(f"{EMOJI_CACHE['cross']} Không có người dùng nào trong cơ sở dữ liệu.", buttons=[[TButton.inline("🔙 QUAY LẠI", b"admin_menu")]])
            except Exception as ex:
                logging.error(f"Lỗi bắn thông báo: {ex}")
                await conv.send_message(f"{EMOJI_CACHE['cross']} Có lỗi hoặc hết hạn chờ.", buttons=[[TButton.inline("🔙 QUAY LẠI", b"admin_menu")]])

    elif data == "admin_notify_top":
        await e.answer()
        if uid != ADMIN_ID: return
        try:
            today_start = datetime.now(VN_TZ).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc).isoformat()
            res = await asyncio.to_thread(lambda: supabase.table("history").select("user_id, amount").eq("action", "Nạp tiền").gte("created_at", today_start).execute())
            
            top_data = {}
            if getattr(res, 'data', None):
                for r in res.data:
                    uid_str = r['user_id']
                    top_data[uid_str] = top_data.get(uid_str, 0) + r['amount']
            
            sorted_top = sorted(top_data.items(), key=lambda x: x[1], reverse=True)[:5]
            
            if not sorted_top:
                await e.edit(f"{EMOJI_CACHE['cross']} Hôm nay chưa có dữ liệu TOP để thông báo.", buttons=[[TButton.inline("🔙 QUAY LẠI ADMIN", b"admin_menu")]])
                return
            
            txt = f"{EMOJI_CACHE['trophy']} **VINH DANH TOP ĐẠI GIA NẠP HÔM NAY** {EMOJI_CACHE['trophy']}\n━━━━━━━━━━━━━━━━━━\n"
            medals = [EMOJI_CACHE['medal1'], EMOJI_CACHE['medal2'], EMOJI_CACHE['medal3'], "🏅", "🏅"]
            for i, (user_id, total_amt) in enumerate(sorted_top):
                txt += f"{medals[i]} Người chơi: `{user_id}` - Tổng nạp: **{total_amt:,}đ**\n"
            txt += "━━━━━━━━━━━━━━━━━━\n🎉 Cảm ơn các anh em đã luôn đồng hành và ủng hộ hệ thống!"
            
            await send_channel_notify(txt)
            await e.edit(f"{EMOJI_CACHE['check']} Đã bắn thông báo TOP lên kênh thành công!", buttons=[[TButton.inline("🔙 QUAY LẠI ADMIN", b"admin_menu")]])
        except Exception as ex:
            logging.error(f"Lỗi bắn thông báo TOP: {ex}")
            await e.edit(f"{EMOJI_CACHE['cross']} Lỗi khi gửi thông báo.", buttons=[[TButton.inline("🔙", b"admin_menu")]])

    elif data == "admin_check_history":
        await e.answer()
        await e.delete()
        async with bot.conversation(uid) as conv:
            try:
                await conv.send_message("🕵️ Nhập ID khách hàng cần kiểm tra lịch sử:")
                check_uid = int((await conv.get_response()).text.strip())
                
                res = await asyncio.to_thread(lambda: supabase.table("history").select("*").eq("user_id", check_uid).order("created_at", desc=True).limit(20).execute())
                if not getattr(res, 'data', None):
                    await conv.send_message(f"{EMOJI_CACHE['cross']} Khách hàng `{check_uid}` không có giao dịch nào trong 24h qua.", buttons=[[TButton.inline("🔙 QUAY LẠI ADMIN", b"admin_menu")]])
                    return
                
                txt = f"🕵️ **LỊCH SỬ CỦA USER: `{check_uid}`**\n━━━━━━━━━━━━━━━━━━\n"
                for h in res.data:
                    dt = datetime.fromisoformat(h['created_at'].replace('Z', '+00:00'))
                    time_str = dt.astimezone(VN_TZ).strftime('%H:%M %d/%m')
                    if h['action'] == "Nạp tiền":
                        txt += f"🔹 `{time_str}` | Nạp tiền: **+{h['amount']:,}đ**\n"
                    else:
                        txt += f"🔸 `{time_str}` | Mua {h['qty']} code {h['game_name']} **(-{h['amount']:,}đ)**\n"
                        if h.get('codes_list'):
                            txt += f"   {EMOJI_CACHE['key']} Code xuất ra: `{h['codes_list']}`\n"
                
                await conv.send_message(txt, buttons=[[TButton.inline("🔙 QUAY LẠI ADMIN", b"admin_menu")]])
            except ValueError:
                await conv.send_message(f"{EMOJI_CACHE['cross']} ID phải là số!", buttons=[[TButton.inline("🔙 QUAY LẠI ADMIN", b"admin_menu")]])
            except Exception as ex:
                logging.error(f"Lỗi admin check lịch sử: {ex}")
                await conv.send_message(f"{EMOJI_CACHE['cross']} Lỗi truy xuất cơ sở dữ liệu!", buttons=[[TButton.inline("🔙 QUAY LẠI ADMIN", b"admin_menu")]])

    elif data == "admin_clones":
        await e.answer()
        if uid != ADMIN_ID: 
            return
        try:
            res = await asyncio.to_thread(lambda: supabase.table("my_clones").select("*").range(0, 1000).execute())
            btns = [[TButton.inline("➕ THÊM CLONE MỚI", b"add_clone")]]
            if getattr(res, 'data', None):
                for c in res.data:
                    status_icon = "🟢" if c['status'] == 'active' else "🔴"
                    btns.append([TButton.inline(f"{status_icon} Xóa {c['phone']}", f"del_clone_{c['id']}")])
            btns.append([TButton.inline("🔙 QUAY LẠI", b"admin_menu")])
            await e.edit(f"📱 **QUẢN LÝ CLONE ({len(res.data) if getattr(res, 'data', None) else 0} acc)** ", buttons=btns)
        except Exception as ex:
            logging.error(f"Lỗi admin_clones: {ex}")
            await e.edit(f"{EMOJI_CACHE['cross']} Lỗi lấy dữ liệu clone.", buttons=[[TButton.inline("🔙", b"admin_menu")]])

    elif data.startswith("del_clone_"):
        try:
            cid = data.split("_")[2]
            await asyncio.to_thread(lambda: supabase.table("my_clones").delete().eq("id", cid).execute())
            await e.answer(f"{EMOJI_CACHE['check']} Đã xóa clone!", alert=True)
            
            res = await asyncio.to_thread(lambda: supabase.table("my_clones").select("*").range(0, 1000).execute())
            btns = [[TButton.inline("➕ THÊM CLONE MỚI", b"add_clone")]]
            if getattr(res, 'data', None):
                for c in res.data:
                    status_icon = "🟢" if c['status'] == 'active' else "🔴"
                    btns.append([TButton.inline(f"{status_icon} Xóa {c['phone']}", f"del_clone_{c['id']}")])
            btns.append([TButton.inline("🔙 QUAY LẠI", b"admin_menu")])
            await e.edit(f"📱 **QUẢN LÝ CLONE ({len(res.data) if getattr(res, 'data', None) else 0} acc)** ", buttons=btns)
        except Exception as ex:
            logging.error(f"Lỗi xóa clone: {ex}")

    elif data == "admin_settings":
        await e.answer()
        if uid != ADMIN_ID: 
            return
        intro = await db_get_setting("BOT_INTRO", "Chưa cài đặt")
        channel = await db_get_setting("NOTIFY_CHANNEL_ID", "Chưa cài đặt")
        support_link = await db_get_setting("SUPPORT_LINK", "https://t.me/admin")
        force_join = await db_get_setting("FORCE_JOIN_CHANNEL", "Chưa cài đặt")
        maint_status = await db_get_setting("MAINTENANCE_MODE", "OFF")
        maint_icon = "🟢 ĐANG BẬT" if maint_status == "ON" else "🔴 ĐANG TẮT"
        
        txt = (f"⚙️ **CÀI ĐẶT HỆ THỐNG** \n\n"
               f"1️⃣ **Lời chào:** {intro}\n"
               f"2️⃣ **ID Kênh thông báo:** `{channel}`\n"
               f"3️⃣ **Link Hỗ Trợ:** `{support_link}`\n"
               f"4️⃣ **Kênh Ép Join:** `{force_join}`\n"
               f"5️⃣ **Trạng thái Bảo Trì:** {maint_status}")
        btns = [
            [TButton.inline("SỬA LỜI CHÀO", b"set_intro"), TButton.inline("SỬA KÊNH THÔNG BÁO", b"set_channel")],
            [TButton.inline("SỬA LINK HỖ TRỢ", b"set_support"), TButton.inline("SỬA KÊNH ÉP JOIN", b"set_force_channel")], 
            [TButton.inline("SỬA QUẢNG CÁO TỰ ĐỘNG (12H)", b"set_auto_ad")],
            [TButton.inline(f"🛠 BẢO TRÌ: {maint_icon}", b"toggle_maintenance")],
            [TButton.inline("🔙 QUAY LẠI", b"admin_menu")]
        ]
        await e.edit(txt, buttons=btns)

    elif data == "toggle_maintenance":
        await e.answer()
        if uid != ADMIN_ID: return
        current = await db_get_setting("MAINTENANCE_MODE", "OFF")
        new_status = "ON" if current == "OFF" else "OFF"
        await db_set_setting("MAINTENANCE_MODE", new_status)
        await e.edit(f"{EMOJI_CACHE['check']} Đã {'BẬT' if new_status == 'ON' else 'TẮT'} chế độ bảo trì thành công!", buttons=[[TButton.inline("🔙 VỀ CÀI ĐẶT", b"admin_settings")]])

    elif data == "set_intro":
        await e.answer()
        await e.delete()
        async with bot.conversation(uid) as conv:
            try:
                await conv.send_message("📝 Nhập lời chào mới:")
                response = await conv.get_response()
                await db_set_setting("BOT_INTRO", response.text.strip())
                await conv.send_message(f"{EMOJI_CACHE['check']} Đã cập nhật thành công!", buttons=[[TButton.inline("🔙 CÀI ĐẶT", b"admin_settings")]])
            except Exception as ex:
                await conv.send_message(f"{EMOJI_CACHE['cross']} Đã quá thời gian chờ hoặc có lỗi xảy ra.")

    elif data == "set_channel":
        await e.answer()
        await e.delete()
        async with bot.conversation(uid) as conv:
            try:
                await conv.send_message(f"{EMOJI_CACHE['speaker']} Nhập ID Kênh (Ví dụ: -100xxx):")
                response = await conv.get_response()
                await db_set_setting("NOTIFY_CHANNEL_ID", response.text.strip())
                await conv.send_message(f"{EMOJI_CACHE['check']} Đã cập nhật thành công!", buttons=[[TButton.inline("🔙 CÀI ĐẶT", b"admin_settings")]])
            except Exception as ex:
                await conv.send_message(f"{EMOJI_CACHE['cross']} Đã quá thời gian chờ hoặc có lỗi xảy ra.")

    elif data == "set_support":
        await e.answer()
        await e.delete()
        async with bot.conversation(uid) as conv:
            try:
                await conv.send_message(f"{EMOJI_CACHE['support']} Nhập Link Hỗ Trợ mới (VD: https://t.me/your_username):")
                response = await conv.get_response()
                await db_set_setting("SUPPORT_LINK", response.text.strip())
                await conv.send_message(f"{EMOJI_CACHE['check']} Đã cập nhật link hỗ trợ thành công!", buttons=[[TButton.inline("🔙 CÀI ĐẶT", b"admin_settings")]])
            except Exception as ex:
                await conv.send_message(f"{EMOJI_CACHE['cross']} Đã quá thời gian chờ hoặc có lỗi xảy ra.")

    elif data == "set_force_channel":
        await e.answer()
        await e.delete()
        async with bot.conversation(uid) as conv:
            try:
                await conv.send_message(f"{EMOJI_CACHE['speaker']} Nhập Username hoặc ID Kênh để ép Join (VD: @kiemtienonline48h hoặc -100123...):\n*(Nhập 'Chưa cài đặt' để tắt tính năng này)*")
                response = await conv.get_response()
                await db_set_setting("FORCE_JOIN_CHANNEL", response.text.strip())
                await conv.send_message(f"{EMOJI_CACHE['check']} Đã cập nhật Kênh Ép Join thành công!", buttons=[[TButton.inline("🔙 CÀI ĐẶT", b"admin_settings")]])
            except Exception as ex:
                await conv.send_message(f"{EMOJI_CACHE['cross']} Đã quá thời gian chờ hoặc có lỗi xảy ra.")

    elif data == "set_auto_ad":
        await e.answer()
        await e.delete()
        async with bot.conversation(uid) as conv:
            try:
                await conv.send_message(f"{EMOJI_CACHE['speaker']} Nhập nội dung QUẢNG CÁO sẽ tự động gửi mỗi 12 tiếng:\n*(Nhập 'Chưa cài đặt' để TẮT tự động quảng cáo)*")
                response = await conv.get_response()
                await db_set_setting("AUTO_AD_MSG", response.text.strip())
                await conv.send_message(f"{EMOJI_CACHE['check']} Đã cập nhật quảng cáo tự động thành công!", buttons=[[TButton.inline("🔙 CÀI ĐẶT", b"admin_settings")]])
            except Exception as ex:
                await conv.send_message(f"{EMOJI_CACHE['cross']} Đã quá thời gian chờ hoặc có lỗi xảy ra.")

    elif data == "admin_cats":
        await e.answer()
        if uid != ADMIN_ID: 
            return
        try:
            cats_res = await asyncio.to_thread(lambda: supabase.table("categories").select("*").execute())
            cats = cats_res.data
            
            if not cats:
                txt = "📂 **DANH SÁCH GAME CỦA SHOP** \n\n❌ Hiện tại kho chưa có game nào. Hãy thêm mới!"
            else:
                txt = "📂 **DANH SÁCH GAME CỦA SHOP** \n━━━━━━━━━━━━━━━━━━\n"
                for c in cats:
                    try:
                        count_res = await asyncio.to_thread(lambda: supabase.table("codes").select("id", count='exact').eq("category_id", c['id']).eq("status", "available").limit(1).execute())
                        stock = count_res.count if count_res.count is not None else 0
                    except Exception as count_err:
                        logging.error(f"Lỗi đếm code danh mục {c['id']}: {count_err}")
                        stock = "Lỗi"

                    owner_label = f"🧑‍💼 Của CTV: {c['owner_id']}" if c.get('owner_id') and c['owner_id'] != 0 else "👑 Của Admin"
                    
                    txt += f"🔸 **ID: `{c['id']}`** | **{c['name']}**\n"
                    txt += f"   ┣ 💵 Giá bán: {c['price']:,}đ\n"
                    txt += f"   ┣ 🏷 Thuộc: {owner_label}\n"
                    txt += f"   ┗ {EMOJI_CACHE['box']} Tồn kho: {stock} code\n"
                    txt += "━━━━━━━━━━━━━━━━━━\n"
            
            btns = [
                [TButton.inline("➕ THÊM GAME MỚI", b"add_cat"), TButton.inline(f"{EMOJI_CACHE['box']} THÊM CODE TAY", b"add_manual_codes")],
                [TButton.inline("✏️ SỬA GIÁ BÁN", b"edit_cat_price"), TButton.inline("🗑 XÓA GAME", b"del_cat")],
                [TButton.inline("🔙 QUAY LẠI ADMIN", b"admin_menu")]
            ]
            await e.edit(txt, buttons=btns)
        except Exception as ex:
            logging.error(f"Lỗi tải danh mục admin: {ex}")
            await e.edit(f"{EMOJI_CACHE['cross']} Lỗi truy xuất cơ sở dữ liệu.", buttons=[[TButton.inline("🔙 QUAY LẠI", b"admin_menu")]])

    elif data == "add_cat":
        await e.answer()
        await e.delete()
        async with bot.conversation(uid) as conv:
            try:
                await conv.send_message(f"{EMOJI_CACHE['game']} Nhập Tên Game Mới:")
                name = (await conv.get_response()).text.strip()
                
                await conv.send_message(f"{EMOJI_CACHE['money']} Nhập Giá bán (Chỉ điền số, VD: 15000):")
                price = int((await conv.get_response()).text.strip())
                
                await conv.send_message(f"{EMOJI_CACHE['bot']} Nhập bot mua code (Bỏ chữ @ đi, VD: kiemtienbot):")
                bot_target = (await conv.get_response()).text.strip().replace("@", "")
                
                await conv.send_message("📝 Nhập Mô tả ngắn gọn cho Game:")
                desc = (await conv.get_response()).text.strip()
                
                await asyncio.to_thread(lambda: supabase.table("categories").insert({"name": name, "price": price, "target_bot": bot_target, "description": desc}).execute())
                await conv.send_message(f"{EMOJI_CACHE['check']} Đã tạo game thành công: **{name}**", buttons=[[TButton.inline("🔙 QUAY LẠI DANH MỤC", b"admin_cats")]])
            except ValueError:
                await conv.send_message(f"{EMOJI_CACHE['cross']} Lỗi: Giá bán phải là một con số!", buttons=[[TButton.inline("🔙 LÀM LẠI", b"admin_cats")]])
            except Exception as ex:
                logging.error(f"Lỗi tạo category: {ex}")
                await conv.send_message(f"{EMOJI_CACHE['cross']} Có lỗi xảy ra trong quá trình tạo!", buttons=[[TButton.inline("🔙 LÀM LẠI", b"admin_cats")]])

    elif data == "edit_cat_price":
        await e.answer()
        await e.delete()
        async with bot.conversation(uid) as conv:
            try:
                await conv.send_message("✏️ Nhập ID của game cần sửa giá (Xem ID ở mục Quản lý danh mục):")
                cid = int((await conv.get_response()).text.strip())
                
                await conv.send_message(f"{EMOJI_CACHE['money']} Nhập GIÁ BÁN MỚI (Chỉ ghi số):")
                new_price = int((await conv.get_response()).text.strip())
                
                await asyncio.to_thread(lambda: supabase.table("categories").update({"price": new_price}).eq("id", cid).execute())
                await conv.send_message(f"{EMOJI_CACHE['check']} Đã cập nhật giá mới thành công!", buttons=[[TButton.inline("🔙 QUAY LẠI DANH MỤC", b"admin_cats")]])
            except ValueError:
                await conv.send_message(f"{EMOJI_CACHE['cross']} Lỗi: ID và Giá tiền phải là số!")
            except Exception as ex:
                logging.error(f"Lỗi sửa giá: {ex}")
                await conv.send_message(f"{EMOJI_CACHE['cross']} Lỗi kết nối CSDL!")

    elif data == "del_cat":
        await e.answer()
        await e.delete()
        async with bot.conversation(uid) as conv:
            try:
                await conv.send_message("🗑 Nhập ID game cần XÓA BỎ HOÀN TOÀN:")
                cid = int((await conv.get_response()).text.strip())
                
                await asyncio.to_thread(lambda: supabase.table("codes").delete().eq("category_id", cid).execute())
                await asyncio.to_thread(lambda: supabase.table("categories").delete().eq("id", cid).execute())
                
                await conv.send_message(f"{EMOJI_CACHE['check']} Đã xóa game và toàn bộ code của game đó!", buttons=[[TButton.inline("🔙 QUAY LẠI DANH MỤC", b"admin_cats")]])
            except ValueError:
                await conv.send_message(f"{EMOJI_CACHE['cross']} Lỗi: ID phải là số!")
            except Exception as ex:
                logging.error(f"Lỗi xóa game: {ex}")
                await conv.send_message(f"{EMOJI_CACHE['cross']} Lỗi không thể xóa!")

    elif data == "add_manual_codes":
        await e.answer()
        await e.delete()
        async with bot.conversation(uid) as conv:
            try:
                await conv.send_message(f"{EMOJI_CACHE['box']} Nhập ID Danh mục (Game) muốn thêm code vào:")
                cat_id = int((await conv.get_response()).text.strip())
                
                await conv.send_message("👉 Gửi danh sách code (Mỗi code nằm trên 1 dòng riêng biệt):")
                codes_msg = await conv.get_response()
                raw_codes = codes_msg.text.strip().split('\n')
                
                insert_data = []
                for c in raw_codes:
                    if c.strip():
                        insert_data.append({"code": c.strip(), "status": "available", "source_phone": "Admin", "category_id": cat_id})
                
                if insert_data:
                    await asyncio.to_thread(lambda: supabase.table("codes").insert(insert_data).execute())
                    await conv.send_message(f"{EMOJI_CACHE['check']} Đã nạp thành công {len(insert_data)} code tay!", buttons=[[TButton.inline("🔙 QUAY LẠI DANH MỤC", b"admin_cats")]])
                else:
                    await conv.send_message(f"{EMOJI_CACHE['cross']} Bạn chưa nhập code nào hợp lệ.", buttons=[[TButton.inline("🔙 QUAY LẠI DANH MỤC", b"admin_cats")]])
            except ValueError:
                await conv.send_message(f"{EMOJI_CACHE['cross']} Lỗi: ID Danh mục phải là số!")
            except Exception as ex:
                logging.error(f"Lỗi thêm code tay: {ex}")
                await conv.send_message(f"{EMOJI_CACHE['cross']} Lỗi hệ thống khi thêm code!")

    elif data == "admin_money":
        await e.answer()
        await e.delete()
        async with bot.conversation(uid) as conv:
            try:
                await conv.send_message(f"{EMOJI_CACHE['user']} Nhập ID khách hàng cần cộng/trừ tiền:")
                tid = int((await conv.get_response()).text.strip())
                
                await conv.send_message(f"{EMOJI_CACHE['money']} Nhập số tiền (Cộng thêm thì ghi 50000, Trừ đi thì ghi -50000):")
                amt = int((await conv.get_response()).text.strip())
                
                user = await db_get_user(tid)
                new_balance = user['balance'] + amt
                
                await asyncio.to_thread(lambda: supabase.table("users").update({"balance": new_balance}).eq("user_id", tid).execute())
                await conv.send_message(f"{EMOJI_CACHE['check']} Thành công! Số dư mới của khách {tid} là: {new_balance:,}đ", buttons=[[TButton.inline("🔙 QUAY LẠI ADMIN", b"admin_menu")]])
            except ValueError:
                await conv.send_message(f"{EMOJI_CACHE['cross']} ID và Số tiền phải là chữ số!")
            except Exception as ex:
                logging.error(f"Lỗi cộng tiền admin: {ex}")
                await conv.send_message(f"{EMOJI_CACHE['cross']} Lỗi cơ sở dữ liệu!")

    elif data == "history":
        await e.answer()
        try:
            res = await asyncio.to_thread(lambda: supabase.table("history").select("*").eq("user_id", uid).order("created_at", desc=True).limit(10).execute())
            hist_data = res.data
            
            if not hist_data:
                await e.edit(f"{EMOJI_CACHE['history']} Bạn chưa có giao dịch nào trong 24h qua.", buttons=[[TButton.inline("🔙 QUAY LẠI", b"back")]])
                return
            
            txt = f"{EMOJI_CACHE['history']} **LỊCH SỬ GIAO DỊCH (24H QUA)**\n━━━━━━━━━━━━━━━━━━\n"
            for h in hist_data:
                dt = datetime.fromisoformat(h['created_at'].replace('Z', '+00:00'))
                time_str = dt.astimezone(VN_TZ).strftime('%H:%M %d/%m') 
                
                if h['action'] == "Nạp tiền":
                    txt += f"🔹 `{time_str}` | Nạp tiền: **+{h['amount']:,}đ**\n"
                else:
                    txt += f"🔸 `{time_str}` | Mua **{h['qty']}** {h['game_name']} (-{h['amount']:,}đ)\n"
                    if h.get('codes_list'):
                        txt += f"   {EMOJI_CACHE['key']} Mã Code: `{h['codes_list']}`\n"
            
            txt += "━━━━━━━━━━━━━━━━━━\n*(Dữ liệu lịch sử và mã code sẽ tự động xóa sạch sau 24h để bảo mật)*"
            await e.edit(txt, buttons=[[TButton.inline("🔙 QUAY LẠI", b"back")]])
        except Exception as ex:
            logging.error(f"Lỗi xem lịch sử: {ex}")
            await e.edit(f"{EMOJI_CACHE['cross']} Lỗi tải lịch sử, vui lòng thử lại.", buttons=[[TButton.inline("🔙 QUAY LẠI", b"back")]])

    elif data == "list_categories":
        await e.answer()
        try:
            cats_res = await asyncio.to_thread(lambda: supabase.table("categories").select("*").execute())
            cats = cats_res.data
            
            if not cats: 
                await e.edit(f"{EMOJI_CACHE['cross']} Shop hiện tại chưa có danh mục game nào đang bán.", buttons=[[TButton.inline("🔙 QUAY LẠI", b"back")]])
                return 
            
            btns = []
            for c in cats:
                try:
                    count_res = await asyncio.to_thread(lambda: supabase.table("codes").select("id", count='exact').eq("category_id", c['id']).eq("status", "available").limit(1).execute())
                    stock = count_res.count if count_res.count is not None else 0
                except:
                    stock = 0
                
                status = f"Kho: {stock}" if stock > 0 else "🔴 HẾT HÀNG"
                btns.append([TButton.inline(f"{EMOJI_CACHE['game']} {c['name']} - {c['price']:,}đ ({status})", f"vcat_{c['id']}")])
            
            btns.append([TButton.inline("🔙 QUAY LẠI", b"back")])
            await e.edit(f"{EMOJI_CACHE['cart']} **DANH SÁCH GAME ĐANG BÁN:**", buttons=btns)
        except Exception as ex:
            logging.error(f"Lỗi list_categories: {ex}")
            await e.edit(f"{EMOJI_CACHE['cross']} Lỗi tải danh mục.", buttons=[[TButton.inline("🔙 QUAY LẠI", b"back")]])

    elif data.startswith("vcat_"):
        await e.answer()
        try:
            cid = int(data.split("_")[1])
            cat_res = await asyncio.to_thread(lambda: supabase.table("categories").select("*").eq("id", cid).execute())
            
            if not getattr(cat_res, 'data', None):
                await e.edit(f"{EMOJI_CACHE['cross']} Danh mục này không tồn tại hoặc đã bị xóa.", buttons=[[TButton.inline("🔙 QUAY LẠI", b"list_categories")]])
                return

            cat = cat_res.data[0]
            
            try:
                count_res = await asyncio.to_thread(lambda: supabase.table("codes").select("id", count='exact').eq("category_id", cid).eq("status", "available").limit(1).execute())
                stock = count_res.count if count_res.count is not None else 0
            except:
                stock = 0
                
            lv, discount_rate, _ = await get_user_level_and_discount(uid)
            discount_price = int(cat['price'] * (1 - discount_rate))
                
            txt = (f"{EMOJI_CACHE['game']} **{cat['name']}** \n━━━━━━━━━━━━\n"
                   f"📝 {cat['description']}\n\n")
            
            if discount_rate > 0:
                txt += f"💵 Giá gốc: ~{cat['price']:,}đ~\n"
                txt += f"🔥 Giá VIP {lv}: **{discount_price:,}đ** (Giảm {int(discount_rate*100)}%)\n"
            else:
                txt += f"💵 Giá bán: **{cat['price']:,}đ** \n"
                
            txt += f"{EMOJI_CACHE['box']} Tồn kho hiện tại: **{stock}** code"
            
            btns = [
                [TButton.inline(f"{EMOJI_CACHE['cart']} MUA 1 CODE", f"buy_{cid}_1")],
                [TButton.inline(f"{EMOJI_CACHE['cart']} MUA NHIỀU CODE", f"buycustom_{cid}")],
                [TButton.inline("🔙 QUAY LẠI DANH MỤC", b"list_categories")]
            ]
            await e.edit(txt, buttons=btns)
        except Exception as ex:
            logging.error(f"Lỗi vcat_: {ex}")
            await e.edit(f"{EMOJI_CACHE['cross']} Lỗi truy xuất thông tin game.", buttons=[[TButton.inline("🔙", b"list_categories")]])

    elif data.startswith("buycustom_"):
        await e.answer()
        await e.delete()
        cid = int(data.split("_")[1])
        async with bot.conversation(uid) as conv:
            try:
                await conv.send_message("🔢 Bạn muốn mua bao nhiêu code? Vui lòng nhập số lượng (VD: 5):")
                response = await conv.get_response()
                qty = int(response.text.strip())
                if qty <= 0: raise ValueError
                
                await process_purchase(e, uid, cid, qty, conv)
            except ValueError:
                await conv.send_message(f"{EMOJI_CACHE['cross']} Lỗi: Vui lòng chỉ nhập số lượng hợp lệ!", buttons=[[TButton.inline("🔙 QUAY LẠI", b"list_categories")]])
            except Exception as ex:
                logging.error(f"Lỗi buycustom: {ex}")
                await conv.send_message(f"{EMOJI_CACHE['cross']} Quá thời gian chờ hoặc có lỗi xảy ra.", buttons=[[TButton.inline("🔙", b"list_categories")]])

    elif data.startswith("buy_"):
        await e.answer()
        parts = data.split("_")
        cid = int(parts[1])
        qty = int(parts[2]) 
        await process_purchase(e, uid, cid, qty, None)

    elif data == "dep_menu":
        await e.answer()
        btns = [
            [TButton.inline("💸 Nạp 10,000đ", "p_10000"), TButton.inline("💸 Nạp 20,000đ", "p_20000")],
            [TButton.inline("💸 Nạp 30,000đ", "p_30000"), TButton.inline("💸 Nạp 50,000đ", "p_50000")],
            [TButton.inline("💸 Nạp 100,000đ", "p_100000"), TButton.inline("💸 Nạp 200,000đ", "p_200000")],
            [TButton.inline("🔙 QUAY LẠI TRANG CHỦ", b"back")]
        ]
        await e.edit(f"{EMOJI_CACHE['bank']} **VUI LÒNG CHỌN MỨC TIỀN MUỐN NẠP:** ", buttons=btns)

    elif data.startswith("p_"):
        await e.answer()
        amt = data.split("_")[1]
        qr = f"https://img.vietqr.io/image/MSB-{STK_MSB}-compact2.png?amount={amt}&addInfo=NAP%20{uid}"
        txt = (f"📥 **HƯỚNG DẪN NẠP TIỀN:**\n\n"
               f"{EMOJI_CACHE['bank']} Ngân hàng: **MSB**\n"
               f"💳 Số tài khoản: `{STK_MSB}`\n"
               f"{EMOJI_CACHE['money']} Số tiền: **{int(amt):,}đ**\n"
               f"📝 Nội dung chuyển khoản (BẮT BUỘC): `NAP {uid}`\n\n"
               f"*(Vui lòng bấm nút mở mã QR bên dưới hoặc chuyển khoản đúng nội dung để được cộng tiền tự động 24/7)*")
        await e.edit(txt, buttons=[[TButton.url("🖼 BẤM VÀO ĐÂY ĐỂ MỞ MÃ QR", qr)], [TButton.inline("🔙 QUAY LẠI", b"dep_menu")]])

# ---> CẬP NHẬT: XỬ LÝ CHIA DOANH THU CTV ĐÃ ĐƯỢC FIX LỖI TẬN GỐC
async def process_purchase(e, uid, cid, qty, conv=None):
    try:
        cat_res = await asyncio.to_thread(lambda: supabase.table("categories").select("*").eq("id", cid).execute())
        if not getattr(cat_res, 'data', None):
            msg = f"{EMOJI_CACHE['cross']} Lỗi: Không tìm thấy game này!"
            if conv: await conv.send_message(msg, buttons=[[TButton.inline("🔙 LÀM LẠI", b"list_categories")]])
            else: await e.edit(msg, buttons=[[TButton.inline("🔙 LÀM LẠI", b"list_categories")]])
            return
            
        cat = cat_res.data[0]
        user = await db_get_user(uid)
        
        lv, discount_rate, _ = await get_user_level_and_discount(uid)
        original_cost = cat['price'] * qty
        cost = int(original_cost * (1 - discount_rate))
        
        if user['balance'] < cost: 
            msg = f"{EMOJI_CACHE['cross']} Rất tiếc, số dư của bạn không đủ để thanh toán. Vui lòng nạp thêm tiền!"
            if conv: await conv.send_message(msg, buttons=[[TButton.inline("🔙", b"list_categories")]])
            else: await bot.send_message(uid, msg)
            return
        
        stock_res = await asyncio.to_thread(lambda: supabase.table("codes").select("*").eq("category_id", cid).eq("status", "available").limit(qty).execute())
        stock_data = getattr(stock_res, 'data', [])
        
        if len(stock_data) < qty: 
            msg = f"{EMOJI_CACHE['cross']} Rất tiếc, trong kho chỉ còn {len(stock_data)} code, không đủ số lượng bạn cần!"
            if conv: await conv.send_message(msg, buttons=[[TButton.inline("🔙", b"list_categories")]])
            else: await bot.send_message(uid, msg)
            return
        
        # 1. Trừ tiền người mua
        await asyncio.to_thread(lambda: supabase.table("users").update({"balance": user['balance'] - cost}).eq("user_id", uid).execute())

        # 2. XỬ LÝ CHIA DOANH THU & GHI LỊCH SỬ CHO CTV
        owner_id = cat.get('owner_id', 0)
        # Ép kiểu an toàn sang Integer để DB đọc được chính xác
        if owner_id and int(owner_id) != 0:
            owner_id = int(owner_id) 
            
            # ĐÃ FIX: Tính phí admin 10% như trong ảnh bạn yêu cầu
            admin_fee = int(cost * 0.1) 
            ctv_revenue = cost - admin_fee
            
            if ctv_revenue > 0:
                try:
                    ctv_user = await db_get_user(owner_id)
                    new_ctv_balance = int(ctv_user.get('ctv_balance', 0)) + ctv_revenue
                    
                    # Cộng tiền vào ví CTV
                    await asyncio.to_thread(lambda: supabase.table("users").update({"ctv_balance": new_ctv_balance}).eq("user_id", owner_id).execute())
                    
                    # Ghi nhận lịch sử bán vào bảng mới ctv_history
                    now_str_utc = datetime.now(timezone.utc).isoformat()
                    await asyncio.to_thread(lambda: supabase.table("ctv_history").insert({
                        "ctv_id": owner_id,
                        "buyer_id": uid,
                        "category_name": cat['name'],
                        "qty": qty,
                        "revenue": ctv_revenue,
                        "admin_fee": admin_fee,
                        "created_at": now_str_utc
                    }).execute())

                    # Bắn thông báo cho CTV
                    asyncio.create_task(bot.send_message(
                        owner_id, 
                        f"🎉 **CHÚC MỪNG: BẠN VỪA BÁN ĐƯỢC HÀNG!**\n"
                        f"{EMOJI_CACHE['user']} Khách hàng ID: `{uid}`\n"
                        f"{EMOJI_CACHE['game']} Sản phẩm: {cat['name']} (Số lượng: {qty})\n"
                        f"💵 Khách trả: {cost:,}đ\n"
                        f"⚙️ Phí hệ thống: -{admin_fee:,}đ\n"
                        f"{EMOJI_CACHE['money']} **Doanh thu cộng ví CTV: +{ctv_revenue:,}đ**"
                    ))
                except Exception as ctv_err:
                    logging.error(f"Lỗi chia tiền/ghi lịch sử cho CTV {owner_id}: {ctv_err}")

        try:
            current_total_sold = int(await db_get_setting("TOTAL_CODES_SOLD", "0"))
            await db_set_setting("TOTAL_CODES_SOLD", str(current_total_sold + qty))
        except:
            pass

        order_id = generate_order_id("DH")
        now_str = datetime.now(VN_TZ).strftime('%H:%M:%S %d/%m/%Y')

        vip_bill_str = f" (Đã áp dụng giảm giá VIP {lv})" if lv > 0 else ""
        res_text = (
            f"{EMOJI_CACHE['check']} **THANH TOÁN THÀNH CÔNG!**\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🔖 **Mã Đơn:** `{order_id}`\n"
            f"{EMOJI_CACHE['game']} **Sản phẩm:** {cat['name']}\n"
            f"{EMOJI_CACHE['box']} **Số lượng:** {qty} code\n"
            f"💵 **Thanh toán:** **-{cost:,} VNĐ**{vip_bill_str}\n"
            f"{EMOJI_CACHE['money']} **Số dư còn lại:** **{user['balance'] - cost:,} VNĐ**\n"
            f"⏰ **Thời gian:** `{now_str}`\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"{EMOJI_CACHE['key']} **DANH SÁCH MÃ CODE CỦA BẠN:**\n"
        )
        codes_str_db = ""
        for c in stock_data:
            await asyncio.to_thread(lambda: supabase.table("codes").update({"status": "sold"}).eq("id", c['id']).execute())
            res_text += f"👉 `{c['code']}`\n"
            codes_str_db += f"{c['code']} | "
            
        res_text += "\n*Cảm ơn bạn đã mua hàng! Hãy lưu lại mã đơn để được hỗ trợ khi cần thiết.*"

        await db_add_history(uid, "Mua Code", cat['name'], qty, cost, codes_str_db.strip(" | "))
        
        channel_notify = (
            f"{EMOJI_CACHE['cart']} **ĐƠN HÀNG MỚI THÀNH CÔNG**\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🔖 **Mã Đơn:** `{order_id}`\n"
            f"{EMOJI_CACHE['user']} **Khách hàng ID:** `{uid}`\n"
            f"{EMOJI_CACHE['game']} **Sản phẩm:** **{cat['name']}**\n"
            f"{EMOJI_CACHE['box']} **Số lượng:** **{qty} code**\n"
            f"{EMOJI_CACHE['money']} **Tổng bill:** **-{cost:,} VNĐ**\n"
            f"⏰ **Thời gian:** `{now_str}`\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"{EMOJI_CACHE['check']} *(Hệ thống tự động bảo mật mã code)*"
        )
        await send_channel_notify(channel_notify)
            
        if conv: 
            await conv.send_message(res_text, buttons=[[TButton.inline("🔙 QUAY LẠI TRANG CHỦ", b"back")]])
        else:
            await e.edit(res_text, buttons=[[TButton.inline("🔙 QUAY LẠI TRANG CHỦ", b"back")]])
            
    except Exception as ex:
        logging.error(f"Lỗi xử lý thanh toán mua code: {ex}")
        if conv: await conv.send_message(f"{EMOJI_CACHE['cross']} Lỗi hệ thống khi thanh toán.")

@bot.on(events.CallbackQuery(data=b"add_clone"))
async def add_clone_process(e):
    await e.answer()
    uid = e.sender_id
    if uid != ADMIN_ID: 
        return
        
    async with bot.conversation(uid) as conv:
        try:
            await conv.send_message("📞 Vui lòng nhập Số điện thoại (+84...):")
            phone = (await conv.get_response()).text.strip()
            
            client = TelegramClient(StringSession(), API_ID, API_HASH, loop=loop)
            await client.connect()
            await client.send_code_request(phone)
            
            await conv.send_message("📩 Telegram đã gửi mã OTP. Vui lòng nhập OTP vào đây:")
            otp = (await conv.get_response()).text.strip()
            
            try:
                await client.sign_in(phone, otp)
            except SessionPasswordNeededError:
                await conv.send_message("🔐 Tài khoản có cài Mật khẩu cấp 2 (2FA). Vui lòng nhập Mật khẩu 2FA:")
                password = (await conv.get_response()).text.strip()
                await client.sign_in(password=password)
                
            ss = client.session.save()
            await asyncio.to_thread(lambda: supabase.table("my_clones").insert({"phone": phone, "session": ss, "status": "active"}).execute())
            await conv.send_message(f"{EMOJI_CACHE['check']} Quá trình thêm Clone hoàn tất và thành công!", buttons=[[TButton.inline("🔙 QUẢN LÝ CLONE", b"admin_clones")]])
            
            asyncio.create_task(worker_grab_loop(client, phone))
            
        except Exception as ex:
            logging.error(f"Lỗi thêm clone: {ex}")
            await conv.send_message(f"{EMOJI_CACHE['cross']} Có lỗi xảy ra trong quá trình đăng nhập (Sai sdt, sai OTP, hoặc Timeout).", buttons=[[TButton.inline("🔙", b"admin_clones")]])

# ==================== WEBHOOK & KEEP-ALIVE (TREO 24/7) ====================
@app.route('/sepay-webhook', methods=['POST'])
def webhook():
    try:
        d = request.json
        content = d.get("content", "").upper()
        m = re.search(r'NAP\D*(\d+)', content)
        if m:
            uid = int(m.group(1))
            amt = int(d.get("transferAmount", 0))
            
            user = sync_db_get_user(uid)
            new_balance = user['balance'] + amt
            sync_db_add_history(uid, "Nạp tiền", "Bank", 1, amt)
            supabase.table("users").update({"balance": new_balance}).eq("user_id", uid).execute()
            
            try:
                current_total_dep = int(sync_db_get_setting("TOTAL_DEPOSIT", "0"))
                sync_db_set_setting("TOTAL_DEPOSIT", str(current_total_dep + amt))
            except Exception as e:
                logging.error(f"Lỗi lưu tổng nạp thống kê: {e}")

            referrer_id = user.get('referrer_id')
            if referrer_id:
                try:
                    commission = int(amt * 0.10)
                    ref_user = sync_db_get_user(referrer_id)
                    supabase.table("users").update({"balance": ref_user['balance'] + commission}).eq("user_id", referrer_id).execute()
                    sync_db_add_history(referrer_id, "Hoa hồng", "Giới thiệu", 1, commission) 
                    asyncio.run_coroutine_threadsafe(
                        bot.send_message(
                            referrer_id, 
                            f"🎊 **BẠN VỪA NHẬN ĐƯỢC HOA HỒNG!**\nThành viên do bạn giới thiệu (ID: `{uid}`) vừa nạp {amt:,}đ.\nBạn được cộng tự động **+{commission:,} VNĐ** (10%) vào tài khoản!"
                        ), 
                        loop
                    )
                except Exception as e:
                    logging.error(f"Lỗi xử lý hoa hồng: {e}")

            tx_id = generate_order_id("NAP")
            now_str = datetime.now(VN_TZ).strftime('%H:%M:%S %d/%m/%Y')
            
            notify_text = (
                f"💳 **GIAO DỊCH NẠP TIỀN THÀNH CÔNG**\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🔖 **Mã GD:** `{tx_id}`\n"
                f"👤 **Khách hàng ID:** `{uid}`\n"
                f"💵 **Số tiền nạp:** **+{amt:,} VNĐ**\n"
                f"💬 **Nội dung:** `{content}`\n"
                f"⏰ **Thời gian:** `{now_str}`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"✅ *Hệ thống cộng tiền tự động 24/7*"
            )
            sync_send_channel_notify(notify_text)
            
            user_text = (
                f"🎉 **NẠP TIỀN THÀNH CÔNG!**\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🔖 **Mã Giao Dịch:** `{tx_id}`\n"
                f"💵 **Số tiền:** **+{amt:,} VNĐ**\n"
                f"💰 **Số dư hiện tại:** **{new_balance:,} VNĐ**\n"
                f"⏰ **Thời gian:** `{now_str}`\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"Cảm ơn bạn đã sử dụng dịch vụ của hệ thống! 🚀"
            )
            try:
                asyncio.run_coroutine_threadsafe(bot.send_message(uid, user_text), loop)
            except Exception as e:
                logging.error(f"Lỗi gửi tin nhắn nạp tiền cho user: {e}")
            return jsonify({"status": "success"}), 200
        return jsonify({"status": "ignored"}), 200
    except Exception as e:
        logging.error(f"Lỗi webhook: {e}")
        return jsonify({"status": "error"}), 500

def run_web():
    app.run(host="0.0.0.0", port=10000)

Thread(target=run_web).start()

def keep_alive():
    while True:
        try:
            urllib.request.urlopen("http://127.0.0.1:10000/", timeout=10)
        except Exception as e:
            logging.warning(f"Lỗi ping keep_alive: {e}")
        time.sleep(120) 

Thread(target=keep_alive).start()

async def main():
    await init_emojis()
    await bot.start(bot_token=BOT_TOKEN)
    print("--- BOT IS STARTED AND ONLINE ---")
    
    asyncio.create_task(auto_clean_history())
    asyncio.create_task(auto_daily_reward())
    # KÍCH HOẠT VÒNG LẶP SPAM QUẢNG CÁO MỖI 12 TIẾNG
    asyncio.create_task(auto_broadcast_ad())
    
    try:
        clones_res = await asyncio.to_thread(lambda: supabase.table("my_clones").select("*").eq("status", "active").range(0, 1000).execute())
        clones = clones_res.data if getattr(clones_res, 'data', None) else []
        if clones:
            print(f"--- ĐÃ TÌM THẤY {len(clones)} CLONE TRONG DB. BẮT ĐẦU KÍCH HOẠT ---")
            for c in clones:
                try:
                    cl = TelegramClient(StringSession(c['session']), API_ID, API_HASH, loop=loop)
                    asyncio.create_task(worker_grab_loop(cl, c['phone']))
                except Exception as clone_err: 
                    logging.error(f"Lỗi khởi động clone {c['phone']}: {clone_err}")
        else:
            print("--- KHÔNG CÓ CLONE NÀO ĐỂ CHẠY HOẶC BỊ CHẶN BỞI RLS ---")
    except Exception as db_err:
        logging.error(f"Lỗi tải danh sách clone từ DB: {db_err}")
        
    await bot.run_until_disconnected()

if __name__ == "__main__":
    loop.run_until_complete(main())

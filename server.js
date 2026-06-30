// server.js - خادم دردشة جمعة رمضان باستخدام Socket.IO
// يدعم جميع الأحداث المطلوبة: create_group, join_group, send_message,
// members_update, new_message, error_message

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs').promises;

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },
  pingInterval: 25000,
  pingTimeout: 60000
});

// ------------------ الإعدادات ------------------
const CONFIG = {
  MAX_MESSAGE_LENGTH: 1000,
  RATE_LIMIT_WINDOW: 60000,
  RATE_LIMIT_MAX: 15,
  MAX_MESSAGES_PER_GROUP: 1000,
  DATA_FILE: './data.json'
};

// ------------------ تخزين البيانات ------------------
let groups = {};
let socketMap = new Map();
const rateLimiter = {};

// ------------------ دوال التخزين الدائم ------------------
async function loadData() {
  try {
    const data = await fs.readFile(CONFIG.DATA_FILE, 'utf8');
    const parsed = JSON.parse(data);
    groups = parsed.groups || {};
    for (const code in groups) {
      groups[code].members = new Set(groups[code].members);
    }
    console.log('📂 تم تحميل البيانات من الملف');
  } catch (err) {
    if (err.code === 'ENOENT') {
      console.log('📂 ملف البيانات غير موجود، سيتم إنشاؤه لاحقاً');
      groups = {};
    } else {
      console.error('❌ خطأ في تحميل البيانات:', err);
    }
  }
}

async function saveData() {
  try {
    const dataToSave = { groups: {} };
    for (const code in groups) {
      dataToSave.groups[code] = {
        members: Array.from(groups[code].members),
        messages: groups[code].messages
      };
    }
    await fs.writeFile(CONFIG.DATA_FILE, JSON.stringify(dataToSave, null, 2));
  } catch (err) {
    console.error('❌ خطأ في حفظ البيانات:', err);
  }
}

// تحميل البيانات عند بدء التشغيل
loadData().then(() => {
  setInterval(saveData, 30000);
});

// ------------------ دوال مساعدة ------------------
function generateGroupCode() {
  let code;
  do {
    code = Math.random().toString(36).substring(2, 8).toUpperCase();
  } while (groups[code]);
  return code;
}

// بث قائمة الأعضاء (حدث members_update)
function broadcastMembers(groupCode) {
  const group = groups[groupCode];
  if (!group) return;
  const membersList = Array.from(group.members);
  io.to(groupCode).emit('members_update', membersList);
}

// بث رسالة نظام (حدث new_message)
function broadcastSystemMessage(groupCode, text) {
  const group = groups[groupCode];
  if (!group) return;
  const message = {
    sender: 'النظام',
    text: text,
    type: 'system',
    timestamp: Date.now()
  };
  group.messages.push(message);
  cleanOldMessages(group);
  io.to(groupCode).emit('new_message', message);
  saveData();
}

// تنظيف الرسائل القديمة
function cleanOldMessages(group) {
  if (group.messages.length > CONFIG.MAX_MESSAGES_PER_GROUP) {
    group.messages.splice(0, group.messages.length - CONFIG.MAX_MESSAGES_PER_GROUP);
  }
}

// التحقق من صحة البيانات
function validateString(value, fieldName, maxLength = CONFIG.MAX_MESSAGE_LENGTH) {
  if (typeof value !== 'string') throw new Error(`الحقل ${fieldName} يجب أن يكون نصياً`);
  const trimmed = value.trim();
  if (trimmed.length === 0) throw new Error(`الحقل ${fieldName} لا يمكن أن يكون فارغاً`);
  if (trimmed.length > maxLength) throw new Error(`الحقل ${fieldName} يتجاوز الحد الأقصى (${maxLength})`);
  return trimmed;
}

// التحقق من Rate Limit
function checkRateLimit(socketId) {
  const now = Date.now();
  if (!rateLimiter[socketId]) rateLimiter[socketId] = [];
  rateLimiter[socketId] = rateLimiter[socketId].filter(ts => now - ts < CONFIG.RATE_LIMIT_WINDOW);
  if (rateLimiter[socketId].length >= CONFIG.RATE_LIMIT_MAX) return false;
  rateLimiter[socketId].push(now);
  return true;
}

// ------------------ أحداث Socket.IO ------------------
io.on('connection', (socket) => {
  console.log(`🟢 عميل متصل: ${socket.id}`);

  // 1. حدث create_group
  socket.on('create_group', async (data, callback) => {
    try {
      const username = validateString(data.username, 'اسم المستخدم', 20);
      const groupCode = generateGroupCode();

      groups[groupCode] = {
        members: new Set([username]),
        messages: [{
          sender: 'النظام',
          text: `تم إنشاء مجموعة "لمة رمضان". كود المجموعة: ${groupCode}`,
          type: 'system',
          timestamp: Date.now()
        }]
      };

      socket.join(groupCode);
      socketMap.set(socket.id, { groupCode, username });

      await saveData();

      // إرسال رد للمستخدم مع البيانات الأولية
      callback({
        success: true,
        groupCode,
        members: Array.from(groups[groupCode].members),
        messages: groups[groupCode].messages
      });

      // بث قائمة الأعضاء للمجموعة (حدث members_update)
      broadcastMembers(groupCode);

      console.log(`✅ تم إنشاء مجموعة ${groupCode} بواسطة ${username}`);
    } catch (err) {
      // حدث error_message
      socket.emit('error_message', { error: err.message });
      callback({ success: false, error: err.message });
    }
  });

  // 2. حدث join_group
  socket.on('join_group', async (data, callback) => {
    try {
      const username = validateString(data.username, 'اسم المستخدم', 20);
      const groupCode = validateString(data.groupCode, 'كود المجموعة', 10);

      const group = groups[groupCode];
      if (!group) {
        return callback({ success: false, error: 'المجموعة غير موجودة' });
      }

      if (group.members.has(username)) {
        return callback({ success: false, error: 'هذا الاسم موجود بالفعل في المجموعة' });
      }

      group.members.add(username);
      socket.join(groupCode);
      socketMap.set(socket.id, { groupCode, username });

      await saveData();

      callback({
        success: true,
        groupCode,
        members: Array.from(group.members),
        messages: group.messages
      });

      // إشعار بانضمام عضو (حدث new_message)
      broadcastSystemMessage(groupCode, `انضم ${username} إلى المجموعة`);
      // تحديث قائمة الأعضاء (حدث members_update)
      broadcastMembers(groupCode);

      console.log(`✅ انضم ${username} إلى المجموعة ${groupCode}`);
    } catch (err) {
      socket.emit('error_message', { error: err.message });
      callback({ success: false, error: err.message });
    }
  });

  // 3. حدث send_message
  socket.on('send_message', async (data) => {
    try {
      if (!data.groupCode) throw new Error('كود المجموعة مطلوب');
      if (!data.text) throw new Error('نص الرسالة مطلوب');

      const groupCode = validateString(data.groupCode, 'كود المجموعة', 10);
      const text = validateString(data.text, 'نص الرسالة', CONFIG.MAX_MESSAGE_LENGTH);

      const group = groups[groupCode];
      if (!group) throw new Error('المجموعة غير موجودة');

      const socketInfo = socketMap.get(socket.id);
      if (!socketInfo || socketInfo.groupCode !== groupCode) {
        throw new Error('أنت لست عضواً في هذه المجموعة');
      }

      if (!checkRateLimit(socket.id)) {
        throw new Error('تم تجاوز حد إرسال الرسائل، الرجاء الانتظار');
      }

      const message = {
        id: Date.now(),
        sender: socketInfo.username,
        text: text,
        type: 'user',
        timestamp: Date.now()
      };

      group.messages.push(message);
      cleanOldMessages(group);
      await saveData();

      // بث الرسالة الجديدة (حدث new_message)
      io.to(groupCode).emit('new_message', message);
    } catch (err) {
      // حدث error_message
      socket.emit('error_message', { error: err.message });
    }
  });

  // 4. حدث ping (اختياري للتحقق)
  socket.on('ping', () => {
    socket.emit('pong');
  });

  // 5. عند قطع الاتصال
  socket.on('disconnect', async () => {
    const socketInfo = socketMap.get(socket.id);
    if (socketInfo) {
      const { groupCode, username } = socketInfo;
      const group = groups[groupCode];
      if (group) {
        group.members.delete(username);
        await saveData();

        // تحديث قائمة الأعضاء (حدث members_update)
        broadcastMembers(groupCode);
        // إشعار بمغادرة عضو (حدث new_message)
        broadcastSystemMessage(groupCode, `غادر ${username} المجموعة`);

        if (group.members.size === 0) {
          delete groups[groupCode];
          await saveData();
          console.log(`🗑️ تم حذف المجموعة ${groupCode} (فارغة)`);
        } else {
          console.log(`🔴 غادر ${username} المجموعة ${groupCode}`);
        }
      }
      socketMap.delete(socket.id);
    }
    delete rateLimiter[socket.id];
    console.log(`🔴 عميل غير متصل: ${socket.id}`);
  });
});

// ------------------ نقطة نهاية للتحقق ------------------
app.get('/health', (req, res) => {
  res.json({ status: 'ok', groups: Object.keys(groups).length });
});

// ------------------ تشغيل الخادم ------------------
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 خادم الدردشة يعمل على المنفذ ${PORT}`);
  console.log(`📌 استخدم http://localhost:${PORT} للاتصال`);
});

// تقديم الملفات الثابتة (اختياري)
app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) => {
  res.send(`
    <h1 style="text-align:center; margin-top:50px;">🌙 خادم جمعة رمضان يعمل بنجاح</h1>
    <p style="text-align:center;">السيرفر جاهز للاستخدام مع Socket.IO</p>
    <p style="text-align:center; color: #aaa;">الأحداث المدعومة: create_group, join_group, send_message, members_update, new_message, error_message</p>
  `);
});

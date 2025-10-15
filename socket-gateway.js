
import express from 'express';
import { createServer } from 'http';
import { Server as SocketIOServer } from 'socket.io';
import cors from 'cors';
import jwt from 'jsonwebtoken';
import pkg from 'pg';
const { Pool } = pkg;
import crypto from 'crypto';

// Import LowCard bot
import { processLowCardCommand, isBotActiveInRoom } from './games/lowcard.js';

// Import Sicbo bot
import { handleSicboCommand, handleSicboAdminCommand, ensureBotPresence as ensureSicboBotPresence, isSicboBotActive } from './games/sicbo.js';

// Import Baccarat bot
import { 
  handleBaccaratCommand, 
  handleBaccaratAdminCommand,
  activateBaccaratBot, 
  deactivateBaccaratBot, 
  ensureBaccaratBotPresence,
  isBaccaratBotActive,
  initBaccaratTables 
} from './games/baccarat.js';

// Import ChatMe AI Bot
import { processBotMessage, BOT_USERNAME } from './bot/chatme-bot.js';

// Import Firebase push notification service
import { initializeFirebase, sendNotificationToUser } from './services/firebase.js';

// Import Red Packet System
import { initRedPacketTables } from './redpacket/database.js';
import { setupRedPacketEvents } from './redpacket/socket-handler.js';
import { expireOldPackets } from './redpacket/redpacket.js';

const app = express();
const server = createServer(app);

// Socket.IO Gateway Configuration
const io = new SocketIOServer(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
    credentials: true
  },
  allowEIO3: true,
  transports: ['websocket', 'polling'],
  pingTimeout: 60000,
  pingInterval: 25000,
  upgradeTimeout: 30000,
  maxHttpBufferSize: 1e6
});

const GATEWAY_PORT = process.env.GATEWAY_PORT || 8000;
// Generate a secure random secret if JWT_SECRET is not provided
const JWT_SECRET = process.env.JWT_SECRET || (() => {
  console.warn('⚠️  WARNING: Using default JWT secret. Set JWT_SECRET environment variable for production!');
  return 'your_super_secret_key_for_development_only';
})();
const MAIN_API_URL = process.env.MAIN_API_URL || 'http://0.0.0.0:5000';


// Database configuration with optimized pooling
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false,
  max: 20,
  min: 2,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
  allowExitOnIdle: false
});

// Test database connection
pool.connect((err, client, release) => {
  if (err) {
    console.error('Gateway: Error connecting to database:', err);
  } else {
    console.log('Gateway: Successfully connected to PostgreSQL database');
    release();
  }
});

// Initialize room security tables
const initRoomSecurityTables = async () => {
  try {
    // Table for banned users per room
    await pool.query(`
      CREATE TABLE IF NOT EXISTS room_banned_users (
        id BIGSERIAL PRIMARY KEY,
        room_id VARCHAR(50) NOT NULL,
        banned_user_id INTEGER,
        banned_username VARCHAR(50) NOT NULL,
        banned_by_id INTEGER NOT NULL,
        banned_by_username VARCHAR(50) NOT NULL,
        ban_reason TEXT,
        banned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        expires_at TIMESTAMP,
        is_active BOOLEAN DEFAULT true,
        UNIQUE(room_id, banned_username)
      )
    `);

    // Table for room locks and passwords
    await pool.query(`
      CREATE TABLE IF NOT EXISTS room_security (
        room_id VARCHAR(50) PRIMARY KEY,
        is_locked BOOLEAN DEFAULT false,
        password_hash TEXT,
        locked_by_id INTEGER,
        locked_by_username VARCHAR(50),
        locked_at TIMESTAMP,
        max_members INTEGER DEFAULT 100,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Table for room moderators and permissions
    await pool.query(`
      CREATE TABLE IF NOT EXISTS room_moderators (
        id BIGSERIAL PRIMARY KEY,
        room_id VARCHAR(50) NOT NULL,
        user_id INTEGER NOT NULL,
        username VARCHAR(50) NOT NULL,
        role VARCHAR(20) DEFAULT 'moderator',
        assigned_by_id INTEGER,
        assigned_by_username VARCHAR(50),
        assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_active BOOLEAN DEFAULT true,
        can_ban BOOLEAN DEFAULT true,
        can_kick BOOLEAN DEFAULT true,
        can_mute BOOLEAN DEFAULT true,
        can_lock_room BOOLEAN DEFAULT false,
        UNIQUE(room_id, user_id)
      )
    `);

    // Add indexes for better performance
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_room_moderators_room_id ON room_moderators(room_id);
    `);

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_room_banned_users_room_id ON room_banned_users(room_id);
    `);

    console.log('✅ Room security tables initialized successfully');
  } catch (error) {
    console.error('❌ Error initializing room security tables:', error);
  }
};

// Create private_messages table if it doesn't exist
const initPrivateMessagesTable = async () => {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS private_messages (
        id SERIAL PRIMARY KEY,
        chat_id VARCHAR(255) NOT NULL,
        sender_id INTEGER NOT NULL REFERENCES users(id),
        message TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_read BOOLEAN DEFAULT false
      )
    `);

    // Add is_read column if it doesn't exist (for existing databases)
    try {
      await pool.query(`
        ALTER TABLE private_messages 
        ADD COLUMN IF NOT EXISTS is_read BOOLEAN DEFAULT false
      `);
    } catch (error) {
      // Column might already exist, ignore error
      console.log('is_read column might already exist:', error.message);
    }
  } catch (error) {
    console.error('Error initializing private messages table:', error);
  }
};

// Create bot_room_members table if it doesn't exist
const initBotRoomMembersTable = async () => {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS bot_room_members (
        id SERIAL PRIMARY KEY,
        room_id VARCHAR(50) NOT NULL,
        bot_user_id INTEGER NOT NULL,
        bot_username VARCHAR(50) NOT NULL,
        added_by_id INTEGER NOT NULL,
        added_by_username VARCHAR(50) NOT NULL,
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_active BOOLEAN DEFAULT true,
        UNIQUE(room_id, bot_user_id)
      )
    `);

    // Add index for better performance
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_bot_room_members_room_id ON bot_room_members(room_id);
    `);

    console.log('✅ Bot room members table initialized successfully');
  } catch (error) {
    console.error('❌ Error initializing bot room members table:', error);
  }
};

// Initialize tables on startup
initRoomSecurityTables();
initPrivateMessagesTable();
initBotRoomMembersTable();
initBaccaratTables();

// Initialize Red Packet tables
initRedPacketTables();

// Server-side permission verification functions
const hasPermission = async (userId, username, roomId, action) => {
  try {
    // Get user's role from database (authoritative source)
    const userResult = await pool.query('SELECT role FROM users WHERE id = $1', [userId]);
    if (userResult.rows.length === 0) {
      return false;
    }

    const userRole = userResult.rows[0].role;

    // Global admins can do anything
    if (userRole === 'admin') {
      return true;
    }

    // Check if user is room moderator
    const moderatorResult = await pool.query(`
      SELECT * FROM room_moderators 
      WHERE room_id = $1 AND user_id = $2 AND is_active = true
    `, [roomId, userId]);

    if (moderatorResult.rows.length > 0) {
      const moderator = moderatorResult.rows[0];

      // Check specific permissions for this moderator
      switch (action) {
        case 'ban':
          return moderator.can_ban;
        case 'kick':
          return moderator.can_kick;
        case 'mute':
          return moderator.can_mute;
        case 'lock_room':
          return false; // Moderators cannot lock rooms
        case 'add_bot':
        case 'remove_bot':
          return true; // Moderators can manage bots
        default:
          return false;
      }
    }

    // Check if user is room owner (created the room)
    const roomResult = await pool.query('SELECT created_by FROM rooms WHERE id = $1', [roomId]);
    if (roomResult.rows.length > 0 && roomResult.rows[0].created_by === username) {
      return true; // Room owners have all permissions
    }

    return false; // Regular users have no moderation permissions
  } catch (error) {
    console.error('Error checking permissions:', error);
    return false;
  }
};

const isUserBanned = async (roomId, userId, username) => {
  try {
    const result = await pool.query(`
      SELECT * FROM room_banned_users 
      WHERE room_id = $1 AND (banned_user_id = $2 OR banned_username = $3) 
      AND is_active = true 
      AND (expires_at IS NULL OR expires_at > NOW())
    `, [roomId, userId, username]);

    return result.rows.length > 0;
  } catch (error) {
    console.error('Error checking ban status:', error);
    return false;
  }
};

const isRoomLocked = async (roomId) => {
  try {
    const result = await pool.query('SELECT is_locked, password_hash FROM room_security WHERE room_id = $1', [roomId]);

    if (result.rows.length === 0) {
      return { locked: false, passwordRequired: false };
    }

    const roomSecurity = result.rows[0];
    return {
      locked: roomSecurity.is_locked,
      passwordRequired: roomSecurity.is_locked && roomSecurity.password_hash !== null
    };
  } catch (error) {
    console.error('Error checking room lock status:', error);
    return { locked: false, passwordRequired: false };
  }
};

const verifyRoomPassword = async (roomId, password) => {
  try {
    const bcrypt = await import('bcrypt');

    const result = await pool.query('SELECT password_hash FROM room_security WHERE room_id = $1', [roomId]);

    if (result.rows.length === 0 || !result.rows[0].password_hash) {
      return false;
    }

    return await bcrypt.default.compare(password, result.rows[0].password_hash);
  } catch (error) {
    console.error('Error verifying room password:', error);
    return false;
  }
};

// Ban management functions
const addBanToDatabase = async (roomId, bannedUserId, bannedUsername, bannedById, bannedByUsername, reason = null, expiresInHours = null) => {
  try {
    let expiresAt = null;
    if (expiresInHours) {
      expiresAt = new Date(Date.now() + (expiresInHours * 60 * 60 * 1000));
    }

    const result = await pool.query(`
      INSERT INTO room_banned_users (
        room_id, banned_user_id, banned_username, banned_by_id, banned_by_username, 
        ban_reason, expires_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (room_id, banned_username) 
      DO UPDATE SET 
        banned_by_id = $4,
        banned_by_username = $5,
        ban_reason = $6,
        expires_at = $7,
        banned_at = NOW(),
        is_active = true
      RETURNING *
    `, [roomId, bannedUserId, bannedUsername, bannedById, bannedByUsername, reason, expiresAt]);

    console.log(`✅ User ${bannedUsername} banned from room ${roomId} by ${bannedByUsername}`);
    return result.rows[0];
  } catch (error) {
    console.error('Error adding ban to database:', error);
    return null;
  }
};

const removeBanFromDatabase = async (roomId, unbannedUsername, unbannedById, unbannedByUsername) => {
  try {
    const result = await pool.query(`
      UPDATE room_banned_users 
      SET is_active = false 
      WHERE room_id = $1 AND banned_username = $2 AND is_active = true
      RETURNING *
    `, [roomId, unbannedUsername]);

    if (result.rows.length > 0) {
      console.log(`✅ User ${unbannedUsername} unbanned from room ${roomId} by ${unbannedByUsername}`);
      return result.rows[0];
    } else {
      console.log(`⚠️  No active ban found for ${unbannedUsername} in room ${roomId}`);
      return null;
    }
  } catch (error) {
    console.error('Error removing ban from database:', error);
    return null;
  }
};

const cleanupExpiredBans = async () => {
  try {
    const result = await pool.query(`
      UPDATE room_banned_users 
      SET is_active = false 
      WHERE expires_at IS NOT NULL AND expires_at <= NOW() AND is_active = true
      RETURNING room_id, banned_username
    `);

    if (result.rows.length > 0) {
      console.log(`✅ Cleaned up ${result.rows.length} expired bans`);
      return result.rows;
    }
    return [];
  } catch (error) {
    console.error('Error cleaning up expired bans:', error);
    return [];
  }
};

// Run cleanup every hour
setInterval(cleanupExpiredBans, 60 * 60 * 1000);

// Room lock management functions
const lockRoom = async (roomId, lockingUserId, lockingUsername, password = null) => {
  try {
    const bcrypt = await import('bcrypt');
    let passwordHash = null;

    if (password) {
      passwordHash = await bcrypt.default.hash(password, 10);
    }

    const result = await pool.query(`
      INSERT INTO room_security (room_id, is_locked, password_hash, locked_by_id, locked_by_username, locked_at)
      VALUES ($1, true, $2, $3, $4, NOW())
      ON CONFLICT (room_id) 
      DO UPDATE SET 
        is_locked = true,
        password_hash = $2,
        locked_by_id = $3,
        locked_by_username = $4,
        locked_at = NOW(),
        updated_at = NOW()
      RETURNING *
    `, [roomId, passwordHash, lockingUserId, lockingUsername]);

    console.log(`🔒 Room ${roomId} locked by ${lockingUsername}${password ? ' with password' : ''}`);
    return result.rows[0];
  } catch (error) {
    console.error('Error locking room:', error);
    return null;
  }
};

const unlockRoom = async (roomId, unlockingUserId, unlockingUsername) => {
  try {
    const result = await pool.query(`
      UPDATE room_security 
      SET is_locked = false, password_hash = null, updated_at = NOW()
      WHERE room_id = $1
      RETURNING *
    `, [roomId]);

    if (result.rows.length > 0) {
      console.log(`🔓 Room ${roomId} unlocked by ${unlockingUsername}`);
      return result.rows[0];
    } else {
      console.log(`⚠️  Room ${roomId} was not locked`);
      return null;
    }
  } catch (error) {
    console.error('Error unlocking room:', error);
    return null;
  }
};

// Function to save chat message to database
const saveChatMessage = async (roomId, username, content, media = null, messageType = 'message', userRole = 'user', userLevel = 1, isPrivate = false) => {
  try {
    // Get user ID by username
    const userResult = await pool.query('SELECT id FROM users WHERE username = $1', [username]);
    const userId = userResult.rows.length > 0 ? userResult.rows[0].id : null;

    const result = await pool.query(`
      INSERT INTO chat_messages (
        room_id, user_id, username, content, media_data,
        message_type, user_role, user_level, is_private
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      RETURNING *
    `, [
      roomId,
      userId,
      username,
      content,
      media ? JSON.stringify(media) : null,
      messageType,
      userRole,
      userLevel,
      isPrivate
    ]);

    console.log(`💾 Gateway: Message saved to database from ${username} in room ${roomId}`);
    return result.rows[0];
  } catch (error) {
    console.error('Gateway: Error saving chat message:', error);
    return null;
  }
};

// Middleware
app.use(cors());
app.use(express.json());

// Simple health check for gateway
app.get('/health', (req, res) => {
  res.json({ 
    message: 'Socket Gateway is running!',
    port: GATEWAY_PORT,
    timestamp: new Date().toISOString()
  });
});

// HTTP endpoint to emit notifications from API server
app.post('/emit-notification', express.json(), (req, res) => {
  try {
    const { userId, notification } = req.body;
    
    if (!userId || !notification) {
      return res.status(400).json({ error: 'userId and notification are required' });
    }

    // Emit notification to user's personal room
    const personalRoom = `user_${userId}`;
    io.to(personalRoom).emit('new_notification', notification);
    
    console.log(`🔔 Notification emitted to ${personalRoom}:`, notification.type);
    res.json({ success: true, message: 'Notification emitted successfully' });
  } catch (error) {
    console.error('Error emitting notification:', error);
    res.status(500).json({ error: 'Failed to emit notification' });
  }
});

// HTTP endpoint to notify user about new private chat
app.post('/gateway/notify-private-chat', express.json(), (req, res) => {
  try {
    const { chatId, recipientId, recipientUsername, initiatorId, initiatorUsername, isNewChat } = req.body;
    
    if (!chatId || !recipientId || !initiatorUsername) {
      return res.status(400).json({ error: 'chatId, recipientId, and initiatorUsername are required' });
    }

    // Send notification to recipient's personal room to open private chat tab
    const personalRoom = `user_${recipientId}`;
    const notification = {
      type: 'private_chat',
      chatId,
      fromUserId: initiatorId,
      fromUsername: initiatorUsername,
      message: `${initiatorUsername} ${isNewChat ? 'started' : 'sent you'} a private chat`,
      timestamp: new Date().toISOString()
    };

    io.to(personalRoom).emit('open_private_chat', notification);
    
    console.log(`📨 Private chat notification sent to ${recipientUsername} (room: ${personalRoom})`);
    console.log(`   Chat ID: ${chatId}, Initiator: ${initiatorUsername}`);
    res.json({ success: true, message: 'Private chat notification sent' });
  } catch (error) {
    console.error('Error sending private chat notification:', error);
    res.status(500).json({ error: 'Failed to send private chat notification' });
  }
});

// HTTP endpoint to get room participants for API server
app.get('/gateway/rooms/:roomId/participants', (req, res) => {
  try {
    const { roomId } = req.params;
    const participants = roomParticipants[roomId] || [];
    res.json(participants);
  } catch (error) {
    console.error('Error fetching room participants:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// In-memory storage for active rooms and participants
const roomParticipants = {}; // { roomId: [ { id, username, role, socketId }, ... ] }
const connectedUsers = new Map(); // socketId -> { userId, username, roomId }
// Track recent leave broadcasts to prevent duplicates (key: "userId_roomId", value: timestamp)
const recentLeaveBroadcasts = new Map();
// Track announced joins per user per room (key: "userId_roomId", value: timestamp) - GLOBAL tracking
const announcedJoins = new Map();
// Removed global joinedRoomsRef - caused join suppression bugs across users

// Debounce system for join/leave broadcasts to prevent spam
const pendingBroadcasts = new Map(); // key: "userId_roomId", value: { type: 'join'|'leave', timeout: timeoutId, data: {...} }
// Track recently broadcast messages to prevent duplicates (key: messageId, value: timestamp)
const recentBroadcasts = new Map();

// Anti-flood rate limiting system
const messageRateLimiter = new Map(); // userId -> array of message timestamps
const cooldownUsers = new Map(); // userId -> cooldown end timestamp

// Rate limit configuration
const RATE_LIMIT = {
  MAX_MESSAGES: 5,        // Maximum messages allowed
  TIME_WINDOW: 5000,      // Time window in milliseconds (5 seconds)
  COOLDOWN_DURATION: 30000 // Cooldown duration in milliseconds (30 seconds)
};

// Function to check if user is rate limited
function checkRateLimit(userId, username) {
  const now = Date.now();
  
  // Check if user is in cooldown
  if (cooldownUsers.has(userId)) {
    const cooldownEnd = cooldownUsers.get(userId);
    if (now < cooldownEnd) {
      const remainingSeconds = Math.ceil((cooldownEnd - now) / 1000);
      return {
        allowed: false,
        reason: 'cooldown',
        remainingSeconds
      };
    } else {
      // Cooldown expired, remove from map
      cooldownUsers.delete(userId);
      messageRateLimiter.delete(userId);
    }
  }
  
  // Get user's message history
  let messageHistory = messageRateLimiter.get(userId) || [];
  
  // Remove old timestamps outside time window
  messageHistory = messageHistory.filter(timestamp => now - timestamp < RATE_LIMIT.TIME_WINDOW);
  
  // Check if user exceeded rate limit
  if (messageHistory.length >= RATE_LIMIT.MAX_MESSAGES) {
    // Apply cooldown
    const cooldownEnd = now + RATE_LIMIT.COOLDOWN_DURATION;
    cooldownUsers.set(userId, cooldownEnd);
    console.log(`🚫 RATE LIMIT: ${username} (ID: ${userId}) exceeded limit - cooldown for ${RATE_LIMIT.COOLDOWN_DURATION / 1000}s`);
    
    return {
      allowed: false,
      reason: 'exceeded',
      cooldownSeconds: RATE_LIMIT.COOLDOWN_DURATION / 1000
    };
  }
  
  // Add current timestamp and update history
  messageHistory.push(now);
  messageRateLimiter.set(userId, messageHistory);
  
  return { allowed: true };
}

// Helper function to sync participant count to database
async function syncParticipantCountToDatabase(roomId) {
  if (!roomId || roomId.startsWith('private_')) {
    return; // Skip private chats
  }
  
  try {
    const participantCount = roomParticipants[roomId] ? roomParticipants[roomId].length : 0;
    await pool.query(
      'UPDATE rooms SET members = $1 WHERE id = $2',
      [participantCount, roomId]
    );
    console.log(`📊 Updated database: Room ${roomId} now has ${participantCount} members`);
  } catch (error) {
    console.error(`❌ Error syncing participant count for room ${roomId}:`, error);
  }
}

// Helper function to schedule debounced join/leave broadcast
function scheduleBroadcast(io, type, userId, roomId, username, role, socket) {
  const key = `${userId}_${roomId}`;
  const DEBOUNCE_DELAY = 2000; // 2 seconds delay
  
  // If there's a pending broadcast of opposite type, cancel it
  const pending = pendingBroadcasts.get(key);
  if (pending) {
    if (pending.type !== type) {
      // Cancel opposite broadcast (e.g., join cancels leave, leave cancels join)
      clearTimeout(pending.timeout);
      pendingBroadcasts.delete(key);
      console.log(`🚫 Cancelled pending ${pending.type} broadcast for ${username} in room ${roomId} (replaced by ${type})`);
      
      // If this is a join after a pending leave, don't broadcast anything (user just reconnected)
      if (type === 'join' && pending.type === 'leave') {
        console.log(`↩️ User ${username} reconnected to room ${roomId} - no broadcast needed`);
        return;
      }
      // If this is a leave after a pending join, don't broadcast anything (user left immediately)
      if (type === 'leave' && pending.type === 'join') {
        console.log(`⚡ User ${username} left room ${roomId} immediately - no broadcast needed`);
        return;
      }
    } else {
      // Same type already pending, just reset the timer
      clearTimeout(pending.timeout);
      console.log(`🔄 Resetting ${type} broadcast timer for ${username} in room ${roomId}`);
    }
  }
  
  // Schedule new broadcast
  const timeout = setTimeout(() => {
    // Create unique message identifier based on action (not timestamp to prevent duplicates)
    const messageKey = `${type}_${userId}_${roomId}`;
    const now = Date.now();
    const lastBroadcast = recentBroadcasts.get(messageKey);
    const BROADCAST_COOLDOWN = 3000; // 3 seconds minimum between same broadcast
    
    // Check if we already broadcast this exact message recently
    if (lastBroadcast && (now - lastBroadcast) < BROADCAST_COOLDOWN) {
      console.log(`🚫 Skipping duplicate ${type} broadcast for ${username} in room ${roomId} (already sent ${Math.round((now - lastBroadcast) / 1000)}s ago)`);
      pendingBroadcasts.delete(key);
      return;
    }
    
    const message = {
      id: `${type}_${Date.now()}_${username}_${roomId}`,
      sender: username,
      content: `${username} ${type === 'join' ? 'joined' : 'left'} the room`,
      timestamp: new Date().toISOString(),
      roomId: roomId,
      type: type,
      userRole: role
    };
    
    if (type === 'join') {
      socket.to(roomId).emit('user-joined', message);
      console.log(`✅ Broadcasting JOIN for ${username} in room ${roomId}`);
    } else {
      io.to(roomId).emit('user-left', message);
      console.log(`✅ Broadcasting LEAVE for ${username} in room ${roomId}`);
    }
    
    // Mark as recently broadcast
    recentBroadcasts.set(messageKey, now);
    
    // Clean up old entries (older than 10 seconds)
    for (const [key, timestamp] of recentBroadcasts.entries()) {
      if (now - timestamp > 10000) {
        recentBroadcasts.delete(key);
      }
    }
    
    pendingBroadcasts.delete(key);
  }, DEBOUNCE_DELAY);
  
  pendingBroadcasts.set(key, { type, timeout, username, roomId });
  console.log(`⏳ Scheduled ${type} broadcast for ${username} in room ${roomId} (${DEBOUNCE_DELAY}ms delay)`);
}

// Socket authentication middleware
const authenticateSocket = async (socket, next) => {
  const token = socket.handshake.auth.token || socket.handshake.headers.authorization?.split(' ')[1];

  if (!token) {
    console.log('Socket connection rejected: No token provided');
    return next(new Error('Authentication error: No token provided'));
  }

  try {
    // Add more detailed JWT validation
    const decoded = jwt.verify(token, JWT_SECRET);
    
    if (!decoded.userId) {
      console.log('Socket authentication failed: Invalid token payload');
      return next(new Error('Authentication error: Invalid token payload'));
    }

    // Get complete user information from database
    const userResult = await pool.query('SELECT id, username, role FROM users WHERE id = $1', [decoded.userId]);

    if (userResult.rows.length === 0) {
      console.log('Socket authentication failed: User not found in database');
      return next(new Error('Authentication error: User not found'));
    }

    const user = userResult.rows[0];

    // Store authenticated user information on socket
    socket.userId = user.id;
    socket.username = user.username;
    socket.userRole = user.role; // This is the authoritative role from database
    socket.authenticated = true;

    console.log(`Socket authenticated for user: ${user.username} (ID: ${user.id}, Role: ${user.role})`);
    next();
  } catch (error) {
    if (error.name === 'TokenExpiredError') {
      console.log('Socket authentication failed: Token expired');
      return next(new Error('Authentication error: Token expired'));
    } else if (error.name === 'JsonWebTokenError') {
      console.log('Socket authentication failed: Invalid token');
      return next(new Error('Authentication error: Invalid token'));
    } else {
      console.log('Socket authentication failed:', error.message);
      return next(new Error('Authentication error: Token validation failed'));
    }
  }
};

// Apply authentication middleware
io.use(authenticateSocket);

// Handle connection errors with better session management
io.engine.on("connection_error", (err) => {
  console.log('❌ Socket connection error:', err.req ? 'Request object present' : 'No request object');
  console.log('❌ Error code:', err.code);
  console.log('❌ Error message:', err.message);
  console.log('❌ Error context:', err.context);
  
  // Handle session ID errors specifically
  if (err.message === 'Session ID unknown' && err.context?.sid) {
    console.log(`🔄 Clearing unknown session: ${err.context.sid}`);
    // Let the client reconnect with a fresh session
  }
});

// Add engine error handling for unknown sessions
io.engine.on("initial_headers", (headers, req) => {
  headers["Access-Control-Allow-Origin"] = "*";
  headers["Access-Control-Allow-Credentials"] = "true";
});

// Handle session validation
io.engine.on("headers", (headers, req) => {
  headers["Access-Control-Allow-Origin"] = "*";
});

io.on('connection', (socket) => {
  console.log(`🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀`);
  console.log(`🚀 GATEWAY CONNECTION ESTABLISHED! THIS IS THE GATEWAY SERVER!`);
  console.log(`🚀 User connected to DEDICATED GATEWAY: ${socket.id}, User ID: ${socket.userId}`);
  console.log(`🚀 Total gateway connections: ${io.sockets.sockets.size}`);
  console.log(`🚀 Time: ${new Date().toISOString()}`);
  console.log(`🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀🚀`);

  // Store connected user info with session tracking
  connectedUsers.set(socket.id, { 
    userId: socket.userId,
    announcedRooms: new Set() // Track rooms where join was already announced
  });

  // Join user to their personal notification room
  const personalRoom = `user_${socket.userId}`;
  socket.join(personalRoom);
  console.log(`🔔 User ${socket.username} joined personal notification room: ${personalRoom}`);

  // Setup Red Packet event handlers
  setupRedPacketEvents(io, socket);

  // Join room event
  socket.on('join-room', async (data) => {
    const { roomId, username, role, password, silent } = data;

    if (!roomId || !username) {
      console.log('❌ Invalid join-room data:', data);
      socket.emit('join-room-error', { error: 'Invalid room data provided' });
      return;
    }

    // SECURITY: Validate that client-provided username matches authenticated identity
    if (username !== socket.username) {
      console.log(`⚠️ Security: User ${socket.username} attempted to join as ${username} in room ${roomId}`);
      socket.emit('join-room-error', { 
        error: 'Authentication mismatch', 
        reason: 'identity_mismatch' 
      });
      return;
    }

    try {
      // 1. Check if user is banned from this room
      const isBanned = await isUserBanned(roomId, socket.userId, socket.username);
      if (isBanned) {
        console.log(`🚫 Banned user ${socket.username} attempted to join room ${roomId}`);
        socket.emit('join-room-error', { 
          error: 'You are banned from this room',
          reason: 'banned'
        });
        return;
      }

      // 2. Check if room is locked
      const roomLockStatus = await isRoomLocked(roomId);
      if (roomLockStatus.locked) {
        if (roomLockStatus.passwordRequired) {
          // Password is required
          if (!password) {
            console.log(`🔒 User ${socket.username} attempted to join locked room ${roomId} without password`);
            socket.emit('join-room-error', { 
              error: 'This room is locked and requires a password',
              reason: 'password_required'
            });
            return;
          }

          // Verify password
          const passwordValid = await verifyRoomPassword(roomId, password);
          if (!passwordValid) {
            console.log(`🔒 User ${socket.username} provided incorrect password for room ${roomId}`);
            socket.emit('join-room-error', { 
              error: 'Incorrect room password',
              reason: 'invalid_password'
            });
            return;
          }
        } else {
          // Room is locked but no password required (admin only)
          const canBypassLock = await hasPermission(socket.userId, socket.username, roomId, 'lock_room');
          if (!canBypassLock && socket.userRole !== 'admin') {
            console.log(`🔒 User ${socket.username} attempted to join locked room ${roomId} without permission`);
            socket.emit('join-room-error', { 
              error: 'This room is locked',
              reason: 'room_locked'
            });
            return;
          }
        }
      }

      // 3. Check if already in room and prevent multiple joins from same user
      const isAlreadyInRoom = socket.rooms.has(roomId);
      const existingParticipant = roomParticipants[roomId]?.find(p => p.userId === socket.userId);

      // Get user's session tracking info
      let userInfo = connectedUsers.get(socket.id);
      if (!userInfo) {
        userInfo = {
          userId: socket.userId,
          announcedRooms: new Set()
        };
        connectedUsers.set(socket.id, userInfo);
      }

      // Check GLOBAL tracking: has this user EVER announced join for this room (from any connection)?
      const joinKey = `${socket.userId}_${roomId}`;
      const hasAnnouncedJoinGlobally = announcedJoins.has(joinKey);

      // Log the join attempt with more detail (use authenticated username)
      if (silent || hasAnnouncedJoinGlobally) {
        console.log(`🔄 ${socket.username} reconnecting to room ${roomId} via gateway (silent)`);
      } else {
        console.log(`🚪 ${socket.username} joining room ${roomId} via gateway (new join)`);
      }

      // Always join the socket room (this is safe to call multiple times)
      socket.join(roomId);

      // Update connected user info (use authenticated identity, not client payload)
      userInfo.roomId = roomId;
      userInfo.username = socket.username; // Use authenticated username
      userInfo.role = socket.userRole;     // Use authenticated role

      // Socket info is already set from authentication - don't overwrite
      // socket.userId and socket.username are set during authentication

      // Add participant to room
      if (!roomParticipants[roomId]) {
        roomParticipants[roomId] = [];
      }

      // Use authenticated identity for participant management
      let participant = roomParticipants[roomId].find(p => p.userId === socket.userId);
      const wasAlreadyParticipant = !!participant;

      // Check if participant was already online BEFORE updating the status
      const wasAlreadyOnline = wasAlreadyParticipant && participant?.isOnline;

      if (participant) {
        // Update existing participant
        participant.isOnline = true;
        participant.socketId = socket.id;
        participant.lastSeen = new Date().toISOString();
        participant.lastActivityTime = Date.now(); // Track activity for 8-hour timeout
        
        console.log(`✅ Updated existing participant: ${socket.username} in room ${roomId}`);
      } else {
        // Add new participant using authenticated identity
        participant = {
          id: Date.now().toString(),
          userId: socket.userId,        // Use authenticated userId
          username: socket.username,    // Use authenticated username
          role: socket.userRole,        // Use authenticated role
          isOnline: true,
          socketId: socket.id,
          joinedAt: new Date().toISOString(),
          lastSeen: new Date().toISOString(),
          lastActivityTime: Date.now()  // Track activity for 8-hour timeout
        };
        roomParticipants[roomId].push(participant);
        console.log(`➕ Added new participant: ${socket.username} to room ${roomId}`);
      }

      // Check if this is a private chat room
      const isPrivateChat = roomId.startsWith('private_');

      // Only broadcast join message if:
      // 1. Not silent (not a reconnection)
      // 2. User was not already online 
      // 3. Join hasn't been announced GLOBALLY for this user+room
      // 4. Not a private chat
      const shouldBroadcastJoin = !silent && !wasAlreadyOnline && !hasAnnouncedJoinGlobally && !isPrivateChat;

      if (shouldBroadcastJoin) {
        // Mark as announced GLOBALLY FIRST to prevent race condition duplicates
        announcedJoins.set(joinKey, Date.now());
        userInfo.announcedRooms.add(roomId);
        
        // Use debounced broadcast to prevent spam from rapid reconnects
        scheduleBroadcast(io, 'join', socket.userId, roomId, socket.username, socket.userRole, socket);
      } else {
        if (silent) {
          console.log(`🔇 Silent join - no broadcast for ${socket.username} in room ${roomId}`);
        } else if (isPrivateChat) {
          console.log(`💬 Private chat join - no broadcast for ${socket.username} in room ${roomId}`);
        } else if (hasAnnouncedJoinGlobally) {
          console.log(`🚫 Skipping duplicate join broadcast for ${socket.username} in room ${roomId} (already announced globally)`);
        } else if (wasAlreadyOnline) {
          console.log(`🚫 Skipping duplicate join broadcast for ${socket.username} in room ${roomId} (already online)`);
        }
      }

      // Always update participants list
      io.to(roomId).emit('participants-updated', roomParticipants[roomId]);

      // Sync participant count to database
      syncParticipantCountToDatabase(roomId);

      // Emit successful join confirmation to the user
      socket.emit('join-room-success', { roomId, username });

      // Show SicboBot activation message if bot is active in this room
      ensureSicboBotPresence(io, roomId);
      
      // Show BaccaratBot activation message if bot is active in this room
      ensureBaccaratBotPresence(io, roomId);

    } catch (error) {
      console.error('Error in join-room handler:', error);
      socket.emit('join-room-error', { 
        error: 'Internal server error',
        reason: 'server_error'
      });
    }
  });

  // Leave room event
  socket.on('leave-room', (data) => {
    const { roomId, username, role } = data;

    // SECURITY: Validate that client-provided username matches authenticated identity
    if (username !== socket.username) {
      console.log(`⚠️ Security: User ${socket.username} attempted to leave room as ${username} in room ${roomId}`);
      socket.emit('leave-room-error', { 
        error: 'Authentication mismatch', 
        reason: 'identity_mismatch' 
      });
      return;
    }

    // Validate user is actually in the Socket.IO room
    if (!socket.rooms.has(roomId)) {
      console.log(`⚠️ User ${socket.username} attempted to leave room ${roomId} but is not in Socket.IO room`);
      socket.emit('leave-room-error', { 
        error: 'You are not in this room', 
        reason: 'not_in_room' 
      });
      return;
    }

    // Check if this is a private chat room
    const isPrivateChat = roomId.startsWith('private_');

    // IMPORTANT: Use debounced broadcast to prevent spam from rapid reconnects
    if (!isPrivateChat) {
      // Use debounced broadcast to prevent spam
      scheduleBroadcast(io, 'leave', socket.userId, roomId, socket.username, socket.userRole, socket);
    } else {
      console.log(`💬 Private chat leave - no broadcast for ${socket.username} in room ${roomId}`);
    }

    // NOW leave the Socket.IO room (after emit so sender receives the message)
    socket.leave(roomId);
    console.log(`${socket.username} left room ${roomId} via gateway`);

    // Remove participant from room (use authenticated userId for security)
    if (roomParticipants[roomId]) {
      roomParticipants[roomId] = roomParticipants[roomId].filter(p => p.userId !== socket.userId);
      io.to(roomId).emit('participants-updated', roomParticipants[roomId]);
      
      // Sync participant count to database
      syncParticipantCountToDatabase(roomId);
    }

    // Update connected user info and clear join tracking
    const userInfo = connectedUsers.get(socket.id);
    if (userInfo) {
      userInfo.roomId = null;
      // Remove from announced rooms when leaving
      userInfo.announcedRooms?.delete(roomId);
    }
  });

  // Join support room event
  socket.on('join-support-room', async (data) => {
    const { supportRoomId, isAdmin, silent } = data;

    if (!supportRoomId) {
      console.log('❌ Invalid join-support-room data:', data);
      socket.emit('join-support-room-error', { error: 'Invalid support room data provided' });
      return;
    }

    try {
      socket.join(supportRoomId);
      console.log(`🚪 ${socket.username} joined support room ${supportRoomId} via gateway`);

      // Update connected user info
      let userInfo = connectedUsers.get(socket.id);
      if (userInfo) {
        userInfo.roomId = supportRoomId;
      }

      // Only broadcast join message if not silent
      if (!silent && isAdmin) {
        const adminJoinMessage = {
          id: Date.now().toString(),
          sender: 'System',
          content: `Admin ${socket.username} has joined the support chat`,
          timestamp: new Date().toISOString(),
          roomId: supportRoomId,
          type: 'join'
        };

        socket.to(supportRoomId).emit('admin-joined', { message: adminJoinMessage.content });
      }

      // Emit successful join confirmation
      socket.emit('join-support-room-success', { supportRoomId });

    } catch (error) {
      console.error('Error in join-support-room handler:', error);
      socket.emit('join-support-room-error', { 
        error: 'Internal server error',
        reason: 'server_error'
      });
    }
  });

  // Lock/Unlock room event
  socket.on('lock-room', async (data) => {
    try {
      const { roomId, action, password } = data; // action: 'lock' or 'unlock'

      if (!roomId || !action) {
        console.log('❌ Invalid lock-room data:', data);
        socket.emit('lock-room-error', { error: 'Invalid lock data provided' });
        return;
      }

      // Verify user has permission to lock/unlock rooms
      const hasLockPermission = await hasPermission(socket.userId, socket.username, roomId, 'lock_room');

      if (!hasLockPermission) {
        console.log(`🚫 User ${socket.username} attempted to ${action} room ${roomId} without permission`);
        socket.emit('lock-room-error', { 
          error: 'You do not have permission to lock/unlock this room',
          reason: 'no_permission'
        });
        return;
      }

      if (action === 'lock') {
        // Lock the room
        const result = await lockRoom(roomId, socket.userId, socket.username, password);

        if (result) {
          // Broadcast lock event to room
          io.to(roomId).emit('room-locked', {
            roomId,
            lockedBy: socket.username,
            hasPassword: !!password,
            timestamp: new Date().toISOString()
          });

          // Send system message
          const lockMessage = {
            id: Date.now().toString(),
            sender: 'System',
            content: `🔒 Room locked by ${socket.username}${password ? ' with password' : ''}`,
            timestamp: new Date().toISOString(),
            roomId: roomId,
            type: 'lock'
          };

          io.to(roomId).emit('new-message', lockMessage);
          socket.emit('lock-room-success', { action: 'lock', roomId });

          console.log(`🔒 Room ${roomId} locked by ${socket.username}`);
        } else {
          socket.emit('lock-room-error', { 
            error: 'Failed to lock room',
            reason: 'server_error'
          });
        }

      } else if (action === 'unlock') {
        // Unlock the room
        const result = await unlockRoom(roomId, socket.userId, socket.username);

        if (result) {
          // Broadcast unlock event to room
          io.to(roomId).emit('room-unlocked', {
            roomId,
            unlockedBy: socket.username,
            timestamp: new Date().toISOString()
          });

          // Send system message
          const unlockMessage = {
            id: Date.now().toString(),
            sender: 'System',
            content: `🔓 Room unlocked by ${socket.username}`,
            timestamp: new Date().toISOString(),
            roomId: roomId,
            type: 'unlock'
          };

          io.to(roomId).emit('new-message', unlockMessage);
          socket.emit('lock-room-success', { action: 'unlock', roomId });

          console.log(`🔓 Room ${roomId} unlocked by ${socket.username}`);
        } else {
          socket.emit('lock-room-error', { 
            error: 'Failed to unlock room or room was not locked',
            reason: 'not_locked_or_error'
          });
        }
      }

    } catch (error) {
      console.error('Error in lock-room handler:', error);
      socket.emit('lock-room-error', { 
        error: 'Internal server error',
        reason: 'server_error'
      });
    }
  });

  // Send message event
  socket.on('sendMessage', async (messageData) => {
    try {
      let { roomId, sender, content, role, level, type, gift, tempId, commandType } = messageData;

      if (!roomId || !sender || !content) {
        console.log('❌ Invalid message data:', messageData);
        return;
      }

      console.log(`📨 Gateway relaying message from ${sender} in room ${roomId}: "${content}"`);

      // Anti-flood rate limiting check (skip for admin/mentor)
      if (socket.userId && role !== 'admin' && role !== 'mentor') {
        const rateLimitResult = checkRateLimit(socket.userId, sender);
        
        if (!rateLimitResult.allowed) {
          if (rateLimitResult.reason === 'cooldown') {
            socket.emit('rate-limit-error', {
              error: `Slow down! You're sending messages too fast. Please wait ${rateLimitResult.remainingSeconds} seconds.`,
              remainingSeconds: rateLimitResult.remainingSeconds,
              type: 'cooldown'
            });
            console.log(`⏸️  Rate limit: ${sender} in cooldown, ${rateLimitResult.remainingSeconds}s remaining`);
          } else if (rateLimitResult.reason === 'exceeded') {
            socket.emit('rate-limit-error', {
              error: `You've been temporarily muted for ${rateLimitResult.cooldownSeconds} seconds due to sending too many messages.`,
              cooldownSeconds: rateLimitResult.cooldownSeconds,
              type: 'exceeded'
            });
            console.log(`🚫 Rate limit exceeded: ${sender} muted for ${rateLimitResult.cooldownSeconds}s`);
          }
          return; // Block message
        }
      }

      // Check if this is a private chat
      const isPrivateChat = roomId.startsWith('private_');

      // For private chats, check target user status
      if (isPrivateChat) {
        try {
          // Extract user IDs from room ID (format: private_id1_id2)
          const roomParts = roomId.split('_');
          if (roomParts.length >= 3) {
            const userId1 = parseInt(roomParts[1]);
            const userId2 = parseInt(roomParts[2]);
            const targetUserId = userId1 === socket.userId ? userId2 : userId1;

            // Get target user status
            const targetUserResult = await pool.query('SELECT username, status FROM users WHERE id = $1', [targetUserId]);

            if (targetUserResult.rows.length > 0) {
              const targetUser = targetUserResult.rows[0];
              const targetStatus = targetUser.status || 'online';

              // Send system message based on user status
              if (targetStatus === 'offline') {
                const systemMessage = {
                  id: `system_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
                  sender: 'System',
                  content: `${targetUser.username} is currently offline`,
                  timestamp: new Date().toISOString(),
                  roomId,
                  role: 'system',
                  level: 1,
                  type: 'system'
                };

                io.to(roomId).emit('new-message', systemMessage);
              } else if (targetStatus === 'away') {
                const systemMessage = {
                  id: `system_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
                  sender: 'System',
                  content: `${targetUser.username} is currently away`,
                  timestamp: new Date().toISOString(),
                  roomId,
                  role: 'system',
                  level: 1,
                  type: 'system'
                };

                io.to(roomId).emit('new-message', systemMessage);
              } else if (targetStatus === 'busy') {
                // Don't allow message sending if user is busy
                const errorMessage = {
                  id: `error_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
                  sender: 'System',
                  content: `${targetUser.username} is currently busy and cannot receive messages`,
                  timestamp: new Date().toISOString(),
                  roomId,
                  role: 'system',
                  level: 1,
                  type: 'error'
                };

                socket.emit('new-message', errorMessage);
                return; // Don't process the original message
              }
            }
          }
        } catch (statusError) {
          console.error('Error checking user status:', statusError);
          // Continue with message sending if status check fails
        }
      }

      // For non-private chats, validate user is properly in room
      if (!isPrivateChat) {
        // Check if user is in participant list (using userId for security)
        const userInRoom = roomParticipants[roomId]?.find(p => p.userId === socket.userId && p.isOnline);
        
        if (!userInRoom) {
          console.log(`⚠️ User ${socket.username} (ID: ${socket.userId}) attempted to send message but is not in room ${roomId} participant list`);
          
          const errorMessage = {
            id: `error_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
            sender: 'System',
            content: 'You are not in the room. Please join the room first to send messages.',
            timestamp: new Date().toISOString(),
            roomId,
            role: 'system',
            level: 1,
            type: 'error'
          };
          
          socket.emit('new-message', errorMessage);
          return;
        }
        
        // Check if socket is in Socket.IO room, if not, re-join automatically
        // This handles the case when user switches apps and comes back
        if (!socket.rooms.has(roomId)) {
          console.log(`🔄 Auto-rejoining ${socket.username} to Socket.IO room ${roomId} (was in participant list but not in socket room)`);
          socket.join(roomId);
          // Update participant's socket ID
          userInRoom.socketId = socket.id;
          userInRoom.lastSeen = new Date().toISOString();
        }

        // Check 3: Ensure sender matches authenticated identity
        if (sender !== socket.username) {
          console.log(`⚠️ Security: User ${socket.username} attempted to send message as ${sender} in room ${roomId}`);
          
          const errorMessage = {
            id: `error_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
            sender: 'System',
            content: 'Authentication error. Please refresh and try again.',
            timestamp: new Date().toISOString(),
            roomId,
            role: 'system',
            level: 1,
            type: 'error'
          };
          
          socket.emit('new-message', errorMessage);
          return;
        }
      }

      // Update user's last activity time for 8-hour inactivity timeout
      if (roomParticipants[roomId]) {
        const participant = roomParticipants[roomId].find(p => p.userId === socket.userId);
        if (participant) {
          participant.lastActivityTime = Date.now();
        }
      }

      // Check if this is a special command that needs server-side handling
      const trimmedContent = content.trim();

      // Handle /addbot or /botadd command - Add ChatMe Bot to room
      if (trimmedContent === '/addbot' || trimmedContent === '/addbot chatme_bot' || trimmedContent === '/botadd' || trimmedContent === '/botadd chatme_bot') {
        console.log(`🤖 Processing /addbot command in room ${roomId} by ${sender}`);
        
        try {
          // Get user info
          const userInfo = connectedUsers.get(socket.id);
          if (!userInfo || !userInfo.userId) {
            socket.emit('system-message', {
              content: '❌ Unable to identify user. Please reconnect.',
              timestamp: new Date().toISOString()
            });
            return;
          }

          // Check permissions - only room owner/moderators/admins can add bot
          const hasAddPermission = await hasPermission(userInfo.userId, sender, roomId, 'add_bot');
          if (!hasAddPermission) {
            socket.emit('system-message', {
              content: '❌ Only room owners, moderators, and admins can add the bot.',
              timestamp: new Date().toISOString()
            });
            return;
          }

          // Check if bot is already in room
          const existingMember = await pool.query(`
            SELECT 1 FROM bot_room_members 
            WHERE room_id = $1 AND bot_user_id = $2 AND is_active = true
          `, [roomId, 43]);

          if (existingMember.rows.length > 0) {
            socket.emit('system-message', {
              content: '⚠️ ChatMe Bot is already in this room!',
              timestamp: new Date().toISOString()
            });
            return;
          }

          // Add bot to room
          await pool.query(`
            INSERT INTO bot_room_members (room_id, bot_user_id, bot_username, added_by_id, added_by_username)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (room_id, bot_user_id) 
            DO UPDATE SET is_active = true, added_by_id = $4, added_by_username = $5, added_at = CURRENT_TIMESTAMP
          `, [roomId, 43, 'chatme_bot', userInfo.userId, sender]);

          // Broadcast success message to room
          io.to(roomId).emit('system-message', {
            content: `🤖 ChatMe Bot has joined the room! (Added by ${sender})`,
            timestamp: new Date().toISOString()
          });

          console.log(`✅ ChatMe Bot added to room ${roomId} by ${sender}`);
        } catch (error) {
          console.error('Error adding bot to room:', error);
          socket.emit('system-message', {
            content: '❌ Failed to add ChatMe Bot to room.',
            timestamp: new Date().toISOString()
          });
        }
        return; // Don't process as regular message
      }

      // Handle /removebot or /botremove command - Remove ChatMe Bot from room
      if (trimmedContent === '/removebot' || trimmedContent === '/removebot chatme_bot' || trimmedContent === '/botremove' || trimmedContent === '/botremove chatme_bot') {
        console.log(`🤖 Processing /removebot command in room ${roomId} by ${sender}`);
        
        try {
          // Get user info
          const userInfo = connectedUsers.get(socket.id);
          if (!userInfo || !userInfo.userId) {
            socket.emit('system-message', {
              content: '❌ Unable to identify user. Please reconnect.',
              timestamp: new Date().toISOString()
            });
            return;
          }

          // Check permissions - only room owner/moderators/admins can remove bot
          const hasRemovePermission = await hasPermission(userInfo.userId, sender, roomId, 'remove_bot');
          if (!hasRemovePermission) {
            socket.emit('system-message', {
              content: '❌ Only room owners, moderators, and admins can remove the bot.',
              timestamp: new Date().toISOString()
            });
            return;
          }

          // Check if bot is in room
          const existingMember = await pool.query(`
            SELECT 1 FROM bot_room_members 
            WHERE room_id = $1 AND bot_user_id = $2 AND is_active = true
          `, [roomId, 43]);

          if (existingMember.rows.length === 0) {
            socket.emit('system-message', {
              content: '⚠️ ChatMe Bot is not in this room.',
              timestamp: new Date().toISOString()
            });
            return;
          }

          // Remove bot from room
          await pool.query(`
            UPDATE bot_room_members 
            SET is_active = false 
            WHERE room_id = $1 AND bot_user_id = $2
          `, [roomId, 43]);

          // Broadcast success message to room
          io.to(roomId).emit('system-message', {
            content: `👋 ChatMe Bot has left the room. (Removed by ${sender})`,
            timestamp: new Date().toISOString()
          });

          console.log(`✅ ChatMe Bot removed from room ${roomId} by ${sender}`);
        } catch (error) {
          console.error('Error removing bot from room:', error);
          socket.emit('system-message', {
            content: '❌ Failed to remove ChatMe Bot from room.',
            timestamp: new Date().toISOString()
          });
        }
        return; // Don't process as regular message
      }

      // Handle /roll command
      if (trimmedContent.startsWith('/roll')) {
        console.log(`🎲 Processing /roll command: ${trimmedContent}`);
        const args = trimmedContent.split(' ');
        const max = args[1] ? parseInt(args[1]) : 100;
        const rolled = Math.floor(Math.random() * max) + 1;

        // Create roll result message
        const rollMessage = {
          id: `roll_${Date.now()}_${sender}_${Math.random().toString(36).substr(2, 9)}`,
          sender: 'System',
          content: `🎲 ${sender} rolled: ${rolled} (1-${max})`,
          timestamp: new Date().toISOString(),
          roomId,
          role: 'system',
          level: 1,
          type: 'system'
        };

        // Broadcast roll result to room
        io.to(roomId).emit('new-message', rollMessage);
        console.log(`Roll result broadcasted to room ${roomId}: ${rolled}`);
        return; // Don't process as regular message
      }

      // Handle /f (follow) command
      if (trimmedContent.startsWith('/f ')) {
        console.log(`👥 Processing /f (follow) command from ${sender}: "${trimmedContent}"`);
        const args = trimmedContent.split(/\s+/); // Use regex to split by any whitespace
        const targetUsername = args[1]?.trim();

        console.log(`  - Parsed args:`, args);
        console.log(`  - Target username:`, targetUsername);

        if (!targetUsername) {
          // Send error message privately to sender
          console.log(`  ❌ No target username specified`);
          const userInfo = connectedUsers.get(socket.id);
          if (userInfo && userInfo.userId) {
            socket.emit('new-message', {
              id: `error_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
              sender: 'System',
              content: '❌ Please specify a username. Usage: /f username',
              timestamp: new Date().toISOString(),
              roomId: roomId,
              type: 'system',
              role: 'system',
              isPrivate: true
            });
          }
          return;
        }

        try {
          // Get sender's user ID
          const userInfo = connectedUsers.get(socket.id);
          if (!userInfo || !userInfo.userId) {
            console.error('User info not found for socket:', socket.id);
            return;
          }

          const followerId = userInfo.userId;

          // Find target user by username
          const targetUserResult = await pool.query(
            'SELECT id, username FROM users WHERE LOWER(username) = LOWER($1)',
            [targetUsername]
          );

          if (targetUserResult.rows.length === 0) {
            // User not found
            socket.emit('new-message', {
              id: `error_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
              sender: 'System',
              content: `❌ User "${targetUsername}" not found.`,
              timestamp: new Date().toISOString(),
              roomId: roomId,
              type: 'system',
              role: 'system',
              isPrivate: true
            });
            return;
          }

          const targetUser = targetUserResult.rows[0];
          const targetUserId = targetUser.id;

          // Check if user is trying to follow themselves
          if (followerId === targetUserId) {
            socket.emit('new-message', {
              id: `error_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
              sender: 'System',
              content: '❌ You cannot follow yourself.',
              timestamp: new Date().toISOString(),
              roomId: roomId,
              type: 'system',
              role: 'system',
              isPrivate: true
            });
            return;
          }

          // Check if already following
          const existingFollow = await pool.query(
            'SELECT id FROM user_follows WHERE follower_id = $1 AND following_id = $2',
            [followerId, targetUserId]
          );

          if (existingFollow.rows.length > 0) {
            // Already following
            socket.emit('new-message', {
              id: `error_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
              sender: 'System',
              content: `ℹ️ You are already following ${targetUser.username}.`,
              timestamp: new Date().toISOString(),
              roomId: roomId,
              type: 'system',
              role: 'system',
              isPrivate: true
            });
            return;
          }

          // Insert follow relationship
          await pool.query(
            'INSERT INTO user_follows (follower_id, following_id, created_at) VALUES ($1, $2, NOW())',
            [followerId, targetUserId]
          );

          console.log(`✅ ${sender} (ID: ${followerId}) followed ${targetUser.username} (ID: ${targetUserId})`);

          // Send success message to sender
          socket.emit('new-message', {
            id: `success_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
            sender: 'System',
            content: `✅ You are now following ${targetUser.username}.`,
            timestamp: new Date().toISOString(),
            roomId: roomId,
            type: 'system',
            role: 'system',
            isPrivate: true
          });

          // Send notification to target user
          const notification = {
            id: `notif_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
            type: 'follow',
            message: `${sender} is followed you`,
            timestamp: new Date().toISOString(),
            data: {
              followerUsername: sender,
              followerId: followerId
            }
          };

          // Emit notification via user's personal room
          io.to(`user_${targetUserId}`).emit('new_notification', notification);
          
          console.log(`🔔 Follow notification sent to ${targetUser.username} (ID: ${targetUserId})`);

        } catch (error) {
          console.error('❌ Error processing /f command:', error);
          console.error('  - Error details:', error.message);
          console.error('  - Stack trace:', error.stack);
          socket.emit('new-message', {
            id: `error_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
            sender: 'System',
            content: `❌ Failed to follow user. Error: ${error.message || 'Unknown error'}`,
            timestamp: new Date().toISOString(),
            roomId: roomId,
            type: 'system',
            role: 'system',
            isPrivate: true
          });
        }

        console.log(`✅ /f command processing completed for ${sender}`);
        return; // Don't process as regular message
      }

      // Handle /broadcast command (admin only)
      if (trimmedContent.startsWith('/broadcast')) {
        console.log(`📢 Processing broadcast command from ${sender}`);
        
        try {
          // Get user info from connected users
          const userInfo = connectedUsers.get(socket.id);
          if (!userInfo || !userInfo.userId) {
            socket.emit('new-message', {
              id: `error_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
              sender: 'System',
              content: '❌ User information not found',
              timestamp: new Date().toISOString(),
              roomId: roomId,
              type: 'system',
              role: 'system',
              isPrivate: true
            });
            return;
          }

          // Verify admin status from database (authoritative source)
          const userResult = await pool.query('SELECT role, username FROM users WHERE id = $1', [userInfo.userId]);
          
          if (userResult.rows.length === 0) {
            socket.emit('new-message', {
              id: `error_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
              sender: 'System',
              content: '❌ User not found in database',
              timestamp: new Date().toISOString(),
              roomId: roomId,
              type: 'system',
              role: 'system',
              isPrivate: true
            });
            return;
          }

          const userRole = userResult.rows[0].role;
          const actualUsername = userResult.rows[0].username;

          // Check if user is admin
          if (userRole !== 'admin') {
            socket.emit('new-message', {
              id: `error_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
              sender: 'System',
              content: '❌ Only admins can use /broadcast command',
              timestamp: new Date().toISOString(),
              roomId: roomId,
              type: 'system',
              role: 'system',
              isPrivate: true
            });
            console.log(`🚫 Broadcast rejected: ${actualUsername} is not admin (role: ${userRole})`);
            return;
          }

          // Check if command is /broadcast off
          if (trimmedContent === '/broadcast off') {
            // Clear broadcast message from database
            await pool.query('UPDATE rooms SET broadcast_message = NULL WHERE id = $1', [roomId]);
            
            // Notify all users in the room to clear broadcast
            io.to(roomId).emit('broadcast-updated', { 
              roomId, 
              broadcastMessage: null 
            });
            
            console.log(`🔇 Broadcast cleared in room ${roomId} by ${actualUsername}`);
            return;
          }

          // Extract message content after /broadcast
          const broadcastContent = content.substring('/broadcast '.length).trim();
          
          if (!broadcastContent) {
            socket.emit('new-message', {
              id: `error_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
              sender: 'System',
              content: '❌ Broadcast message cannot be empty. Use /broadcast off to clear',
              timestamp: new Date().toISOString(),
              roomId: roomId,
              type: 'system',
              role: 'system',
              isPrivate: true
            });
            return;
          }

          // Save broadcast message to database
          await pool.query('UPDATE rooms SET broadcast_message = $1 WHERE id = $2', [broadcastContent, roomId]);

          // Notify all users in the room about new broadcast
          io.to(roomId).emit('broadcast-updated', { 
            roomId, 
            broadcastMessage: broadcastContent 
          });
          
          console.log(`📢 Broadcast message saved and sent to room ${roomId} by ${actualUsername}: ${broadcastContent}`);
          
        } catch (error) {
          console.error('❌ Error processing /broadcast command:', error);
          socket.emit('new-message', {
            id: `error_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
            sender: 'System',
            content: `❌ Failed to broadcast message. Error: ${error.message || 'Unknown error'}`,
            timestamp: new Date().toISOString(),
            roomId: roomId,
            type: 'system',
            role: 'system',
            isPrivate: true
          });
        }
        
        return; // Don't process as regular message
      }

      // Handle /lock and /unlock commands
      if (trimmedContent.startsWith('/lock') || trimmedContent === '/unlock') {
        try {
          const userInfo = connectedUsers.get(socket.id);
          if (!userInfo || !userInfo.userId) {
            socket.emit('new-message', {
              id: `error_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
              sender: 'System',
              content: '❌ User not found',
              timestamp: new Date().toISOString(),
              roomId: roomId,
              type: 'system',
              role: 'system',
              isPrivate: true
            });
            return;
          }

          // Check permission
          const hasLockPermission = await hasPermission(userInfo.userId, userInfo.username, roomId, 'lock_room');
          
          if (!hasLockPermission) {
            socket.emit('new-message', {
              id: `error_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
              sender: 'System',
              content: '❌ You do not have permission to lock/unlock this room',
              timestamp: new Date().toISOString(),
              roomId: roomId,
              type: 'system',
              role: 'system',
              isPrivate: true
            });
            return;
          }

          if (trimmedContent === '/unlock') {
            // Unlock the room
            const result = await unlockRoom(roomId, userInfo.userId, userInfo.username);
            
            if (result) {
              // Broadcast unlock event
              io.to(roomId).emit('room-unlocked', {
                roomId,
                unlockedBy: userInfo.username,
                timestamp: new Date().toISOString()
              });

              // Send system message
              const unlockMessage = {
                id: Date.now().toString(),
                sender: 'System',
                content: `🔓 Room unlocked by ${userInfo.username}`,
                timestamp: new Date().toISOString(),
                roomId: roomId,
                type: 'unlock'
              };

              io.to(roomId).emit('new-message', unlockMessage);
              console.log(`🔓 Room ${roomId} unlocked by ${userInfo.username}`);
            }
          } else {
            // Extract password from /lock command
            const password = content.substring('/lock'.length).trim() || null;

            // Lock the room
            const result = await lockRoom(roomId, userInfo.userId, userInfo.username, password);

            if (result) {
              // Broadcast lock event to room
              io.to(roomId).emit('room-locked', {
                roomId,
                lockedBy: userInfo.username,
                hasPassword: !!password,
                timestamp: new Date().toISOString()
              });

              // Send system message
              const lockMessage = {
                id: Date.now().toString(),
                sender: 'System',
                content: `🔒 Room locked by ${userInfo.username}${password ? ' with password' : ''}`,
                timestamp: new Date().toISOString(),
                roomId: roomId,
                type: 'lock'
              };

              io.to(roomId).emit('new-message', lockMessage);
              console.log(`🔒 Room ${roomId} locked by ${userInfo.username}${password ? ' with password' : ''}`);
            }
          }
        } catch (error) {
          console.error('❌ Error processing lock/unlock command:', error);
          socket.emit('new-message', {
            id: `error_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
            sender: 'System',
            content: `❌ Failed to ${trimmedContent.startsWith('/lock') ? 'lock' : 'unlock'} room`,
            timestamp: new Date().toISOString(),
            roomId: roomId,
            type: 'system',
            role: 'system',
            isPrivate: true
          });
        }
        
        return; // Don't process as regular message
      }

      // Handle bot commands
      if (trimmedContent.startsWith('/bot lowcard add') || 
          trimmedContent.startsWith('/bot sicbo') ||
          trimmedContent.startsWith('/bot bacarat') ||
          trimmedContent.startsWith('/add') || 
          trimmedContent.startsWith('/init_bot') ||
          trimmedContent.startsWith('/bot off') ||
          trimmedContent.startsWith('!')) {

        console.log(`🤖 Processing bot command: "${trimmedContent}" (length: ${trimmedContent.length}, from: ${sender})`);

        // Get user info from connected users
        const userInfo = connectedUsers.get(socket.id);
        if (userInfo && userInfo.userId) {
          // Handle Baccarat admin commands (add/remove bot)
          if (trimmedContent.includes('bacarat') && trimmedContent.startsWith('/')) {
            console.log(`[Baccarat] Routing to admin command handler: ${trimmedContent}`);
            const handled = await handleBaccaratAdminCommand(io, roomId, trimmedContent, userInfo.userId, sender, socket.userRole);
            console.log(`[Baccarat] Admin command handled: ${handled}`);
            if (handled) {
              return; // Command was handled by Baccarat admin handler
            }
          }

          // Handle Baccarat game commands (check if Baccarat bot is active)
          const baccaratActive = isBaccaratBotActive(roomId);
          if (baccaratActive && (
              trimmedContent.startsWith('!bet ') || 
              trimmedContent.startsWith('!b ') ||
              trimmedContent.startsWith('!deal') ||
              trimmedContent.startsWith('!bacarat') ||
              trimmedContent === '!start' ||
              trimmedContent === '!help' ||
              trimmedContent === '!status')) {
            handleBaccaratCommand(io, socket, roomId, trimmedContent, userInfo.userId, sender);
            return;
          }

          // Check which bot is active
          const sicboActive = isSicboBotActive(roomId);
          const lowcardActive = isBotActiveInRoom(roomId);
          
          // Route to appropriate bot based on command and active status
          // LowCard shortened commands: !start, !j, !d, !leave, !help, !status (only if LowCard is active)
          if (lowcardActive && (
              trimmedContent.startsWith('!start ') || 
              trimmedContent === '!j' ||
              trimmedContent === '!d' ||
              trimmedContent === '!leave' ||
              trimmedContent === '!help' ||
              trimmedContent === '!status')) {
            // LowCard game command (shortened format)
            processLowCardCommand(io, roomId, trimmedContent, userInfo.userId, sender, socket.userRole);
          } else if (sicboActive && (trimmedContent.startsWith('!start') || 
              trimmedContent.startsWith('!s ') || 
              trimmedContent === '!s' ||
              trimmedContent === '!help' ||
              trimmedContent === '!status')) {
            // Sicbo game command (shortened format)
            const args = trimmedContent.slice(1).split(/\s+/);
            const cmd = args.shift(); // Remove '!start', '!s', etc
            
            // Map shortened commands to full commands
            if (cmd === 's') {
              args.unshift('bet'); // !s becomes 'bet' command
            } else if (cmd === 'start') {
              args.unshift('start');
            } else if (cmd === 'help') {
              args.unshift('help');
            } else if (cmd === 'status') {
              args.unshift('status');
            }
            
            handleSicboCommand(io, socket, roomId, args, userInfo.userId, sender);
          } else if (trimmedContent.startsWith('!sicbo')) {
            // Legacy sicbo command (keep for backward compatibility)
            const args = trimmedContent.slice(1).split(/\s+/);
            args.shift(); // Remove '!sicbo'
            handleSicboCommand(io, socket, roomId, args, userInfo.userId, sender);
          } else if (trimmedContent.includes('sicbo') && trimmedContent.startsWith('/')) {
            // Sicbo admin command (/add sicbo, /bot sicbo add, etc.)
            console.log(`[Sicbo] Routing to admin command handler: ${trimmedContent}`);
            const handled = await handleSicboAdminCommand(io, roomId, trimmedContent, userInfo.userId, sender, socket.userRole);
            console.log(`[Sicbo] Admin command handled: ${handled}`);
            if (!handled) {
              // If not handled by Sicbo, try LowCard
              console.log(`[Sicbo] Not handled, passing to LowCard`);
              processLowCardCommand(io, roomId, trimmedContent, userInfo.userId, sender, socket.userRole);
            }
          } else if (
            (trimmedContent.includes('lowcard') && trimmedContent.startsWith('/')) ||
            trimmedContent.startsWith('!lowcard') ||
            trimmedContent === '/add' ||
            trimmedContent === '/addbot' ||
            trimmedContent === '/init_bot' ||
            trimmedContent === '/bot off'
          ) {
            // LowCard admin and game commands
            console.log(`[LowCard] Routing command to processLowCardCommand: ${trimmedContent}`);
            processLowCardCommand(io, roomId, trimmedContent, userInfo.userId, sender, socket.userRole);
          } else {
            // Unknown bot command - send error message to user
            socket.emit('chat-message', {
              id: `system_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
              sender: 'System',
              content: `Unknown command: ${trimmedContent}. Make sure the correct bot is active in this room.`,
              timestamp: new Date().toISOString(),
              roomId: roomId,
              type: 'system',
              role: 'system',
              isPrivate: true
            });
          }
        } else {
          console.error('User info not found for socket:', socket.id);
        }

        // Don't broadcast bot commands as regular messages - make them all private
        return;
      }

      // Create message with unique ID
      const messageId = tempId ? tempId.replace('temp_', '') + '_confirmed' : `${Date.now()}_${sender}_${Math.random().toString(36).substr(2, 9)}`;

      const newMessage = {
        id: messageId,
        sender,
        content,
        timestamp: new Date().toISOString(),
        roomId,
        role: role || 'user',
        level: level || 1,
        type: type || 'message',
        commandType: commandType || null,
        gift: gift || null
      };

      // Save private chat messages to database
      if (isPrivateChat) {
        await saveChatMessage(
          roomId,
          sender,
          content,
          gift, // media data (for gifts)
          type || 'message',
          role || 'user',
          level || 1,
          true // isPrivate
        );
        console.log(`💾 Private chat message saved to database: ${roomId}`);

        // Save to private_messages table for notification tracking
        try {
          const userResult = await pool.query('SELECT id FROM users WHERE username = $1', [sender]);
          const senderId = userResult.rows.length > 0 ? userResult.rows[0].id : null;

          if (senderId) {
            await pool.query(`
              INSERT INTO private_messages (chat_id, sender_id, message, is_read)
              VALUES ($1, $2, $3, false)
            `, [roomId, senderId, content]);
            console.log(`💾 Private message saved for notifications: ${roomId}`);
          }
        } catch (error) {
          console.error('Error saving private message for notifications:', error);
        }
      }

      // Broadcast message to room
      io.to(roomId).emit('new-message', newMessage);

      // If it's a gift, also broadcast animation
      if (type === 'gift' && gift) {
        io.to(roomId).emit('gift-animation', {
          gift,
          sender,
          timestamp: new Date().toISOString()
        });
      }

      console.log(`Message broadcasted to room ${roomId} from ${sender}`);

      // Send push notification for private chats
      if (isPrivateChat && sender) {
        try {
          // Extract user IDs from private chat room ID (format: private_id1_id2)
          const roomParts = roomId.split('_');
          if (roomParts.length >= 3) {
            const userId1 = parseInt(roomParts[1]);
            const userId2 = parseInt(roomParts[2]);
            
            // Get sender ID
            const senderResult = await pool.query('SELECT id FROM users WHERE username = $1', [sender]);
            if (senderResult.rows.length > 0) {
              const senderId = senderResult.rows[0].id;
              
              // Determine recipient ID (the other user in the chat)
              const recipientId = userId1 === senderId ? userId2 : userId1;
              
              // Send push notification to recipient
              await sendNotificationToUser(
                pool,
                recipientId,
                {
                  title: `New message from ${sender}`,
                  body: content.substring(0, 100), // Truncate long messages
                },
                {
                  type: 'private_message',
                  roomId: roomId,
                  senderId: senderId.toString(),
                  senderUsername: sender
                }
              );
              
              console.log(`📱 Push notification sent to user ${recipientId} for private message`);
            }
          }
        } catch (pushError) {
          console.error('❌ Error sending push notification for private message:', pushError);
          // Don't block message sending if push notification fails
        }
      }

      // ChatMe AI Bot Integration
      // Check if bot should respond to this message
      if (sender !== BOT_USERNAME) { // Don't respond to bot's own messages
        try {
          const botResponse = await processBotMessage({
            message: content,
            roomId,
            username: sender,
            conversationHistory: [], // Could fetch recent messages from DB for context
            pool // Pass database pool for membership check
          });

          if (botResponse) {
            // Bot has a response - broadcast it to the room
            const botMessageId = `${Date.now()}_${BOT_USERNAME}_${Math.random().toString(36).substr(2, 9)}`;
            
            const botMessage = {
              id: botMessageId,
              sender: BOT_USERNAME,
              content: botResponse.content,
              timestamp: new Date().toISOString(),
              roomId,
              role: 'user',
              level: 1,
              type: 'message',
              isBot: true // Mark as bot message for frontend styling
            };

            // Small delay to make it feel more natural
            setTimeout(() => {
              io.to(roomId).emit('new-message', botMessage);
              console.log(`🤖 ChatMe Bot responded in room ${roomId}: "${botResponse.content}"`);
              
              // Save bot message to database for private chats
              if (isPrivateChat) {
                saveChatMessage(
                  roomId,
                  BOT_USERNAME,
                  botResponse.content,
                  null,
                  'message',
                  'user',
                  1,
                  true
                ).catch(err => console.error('Error saving bot message:', err));
              }
            }, 800); // 800ms delay for natural conversation feel
          }
        } catch (botError) {
          console.error('❌ ChatMe Bot error:', botError);
          // Don't crash if bot fails - just log the error
        }
      }

    } catch (error) {
      console.error('Error handling sendMessage:', error);
    }
  });

  // Send gift event
  socket.on('sendGift', async (giftData) => {
    try {
      const { roomId, sender, gift, timestamp, role, level, recipient } = giftData;

      if (!roomId || !sender || !gift) {
        console.log('Invalid gift data:', giftData);
        return;
      }

      console.log(`Gateway relaying gift from ${sender} in room ${roomId}: ${gift.name}`);

      // Note: Gift messages are real-time only, not saved to database
      // This keeps chat history clean when users leave rooms

      // Broadcast gift to all users in the room
      io.to(roomId).emit('receiveGift', {
        roomId,
        sender,
        gift,
        recipient,
        timestamp: timestamp || new Date().toISOString(),
        role: role || 'user',
        level: level || 1
      });

      console.log(`Gift broadcasted to room ${roomId} from ${sender}`);

    } catch (error) {
      console.error('Error handling sendGift:', error);
    }
  });

  // Send private gift event
  socket.on('send-private-gift', async (giftData) => {
    try {
      const { from, to, gift, timestamp, roomId } = giftData;

      if (!from || !to || !gift) {
        console.log('❌ Invalid private gift data:', giftData);
        return;
      }

      console.log(`🎁 Gateway relaying private gift from ${from} to ${to}: ${gift.name}`);
      console.log(`🎁 Gift details:`, JSON.stringify(gift, null, 2));

      // Save gift notification message to database for persistence
      if (roomId) {
        try {
          // Include gift icon/emoji in message content for visual display
          const giftIcon = gift.icon || '🎁';
          const messageContent = `${from} send ${gift.name} ${giftIcon} to ${to}`;
          
          const result = await pool.query(
            `INSERT INTO private_messages (chat_id, sender_id, message, created_at) 
             SELECT $1, u.id, $2, $3 
             FROM users u WHERE u.username = $4
             RETURNING id, chat_id, sender_id, message, created_at`,
            [roomId, messageContent, timestamp || new Date().toISOString(), from]
          );
          
          if (result.rows.length > 0) {
            const savedMessage = result.rows[0];
            console.log(`💾 Gift notification message saved to database: ${messageContent}`);
            
            // Emit as regular message so it appears in chat history with gift data
            io.to(roomId).emit('new-message', {
              id: savedMessage.id.toString(),
              roomId: roomId,
              sender: from,
              content: messageContent,
              timestamp: savedMessage.created_at,
              type: 'system',
              gift: {
                icon: giftIcon,
                name: gift.name,
                image: gift.image || gift.imageUrl,
                price: gift.price
              }
            });
          }
        } catch (dbError) {
          console.error('❌ Error saving gift notification to database:', dbError);
        }
      }

      // Gift notification already saved to database and delivered via 'new-message' event
      // No need to emit 'receive-private-gift' separately to prevent duplicate animations
      console.log(`✅ Private gift notification complete (delivered via new-message event)`);

    } catch (error) {
      console.error('❌ Error handling send-private-gift:', error);
    }
  });

  // Typing indicator events
  socket.on('typing-start', (data) => {
    const { roomId, username } = data;
    if (roomId && username) {
      socket.to(roomId).emit('user-typing', { username, typing: true });
    }
  });

  socket.on('typing-stop', (data) => {
    const { roomId, username } = data;
    if (roomId && username) {
      socket.to(roomId).emit('user-typing', { username, typing: false });
    }
  });

  // User moderation events - SECURE VERSION with server-side verification
  socket.on('kick-user', async (data) => {
    try {
      const { roomId, targetUsername, reason } = data;

      if (!roomId || !targetUsername) {
        console.log('❌ Invalid kick-user data:', data);
        socket.emit('kick-user-error', { error: 'Invalid kick data provided' });
        return;
      }

      // 1. SERVER-SIDE PERMISSION VERIFICATION - Use authoritative JWT data
      const hasKickPermission = await hasPermission(socket.userId, socket.username, roomId, 'kick');

      if (!hasKickPermission) {
        console.log(`🚫 User ${socket.username} (ID: ${socket.userId}) attempted to kick ${targetUsername} without permission in room ${roomId}`);
        socket.emit('kick-user-error', { 
          error: 'You do not have permission to kick users in this room',
          reason: 'no_permission'
        });
        return;
      }

      // 2. Get target user information from database (authoritative source)
      const targetUserResult = await pool.query('SELECT id, username, role FROM users WHERE username = $1', [targetUsername]);

      if (targetUserResult.rows.length === 0) {
        socket.emit('kick-user-error', { 
          error: 'Target user not found',
          reason: 'user_not_found'
        });
        return;
      }

      const targetUser = targetUserResult.rows[0];

      // 3. Prevent kicking admins or yourself
      if (targetUser.id === socket.userId) {
        socket.emit('kick-user-error', { 
          error: 'You cannot kick yourself',
          reason: 'cannot_kick_self'
        });
        return;
      }

      if (targetUser.role === 'admin') {
        socket.emit('kick-user-error', { 
          error: 'Cannot kick administrators',
          reason: 'cannot_kick_admin'
        });
        return;
      }

      // Get kicker's role from database for message
      const kickerResult = await pool.query('SELECT role FROM users WHERE id = $1', [socket.userId]);
      const kickerRole = kickerResult.rows[0]?.role || 'moderator';
      const roleText = kickerRole === 'admin' ? 'administrator' : 'moderator';

      // 4. AUTHORITATIVE ENFORCEMENT - Remove from participants
      if (roomParticipants[roomId]) {
        roomParticipants[roomId] = roomParticipants[roomId].filter(p => p.username !== targetUsername);
        io.to(roomId).emit('participants-updated', roomParticipants[roomId]);
      }

      // 5. Force disconnect kicked user from room if they're online
      const kickedUserSocket = [...connectedUsers.entries()].find(([socketId, userInfo]) => 
        userInfo.username === targetUsername && userInfo.roomId === roomId
      );

      if (kickedUserSocket) {
        const [kickedSocketId] = kickedUserSocket;
        io.to(kickedSocketId).emit('user-kicked', {
          roomId,
          kickedUser: targetUsername,
          kickedBy: socket.username,
          reason: reason || 'No reason provided'
        });

        // Force leave the room
        io.to(kickedSocketId).emit('force-leave-room', { 
          roomId, 
          reason: 'kicked',
          kickedBy: socket.username 
        });
      }

      // 6. Broadcast verified kick event to room
      io.to(roomId).emit('user-kicked', {
        roomId,
        kickedUser: targetUsername,
        kickedBy: socket.username // Use server-side authoritative username
      });

      // 7. Send verified system message with role
      const kickMessage = {
        id: Date.now().toString(),
        sender: 'System',
        content: `${targetUsername} was kicked by ${roleText} ${socket.username}`,
        timestamp: new Date().toISOString(),
        roomId: roomId,
        type: 'kick'
      };

      io.to(roomId).emit('new-message', kickMessage);

      // 8. Confirm success to requesting user
      socket.emit('kick-user-success', { targetUsername, roomId });

      console.log(`✅ User ${targetUsername} kicked from room ${roomId} by ${socket.username} (verified)`);

    } catch (error) {
      console.error('Error in kick-user handler:', error);
      socket.emit('kick-user-error', { 
        error: 'Internal server error',
        reason: 'server_error'
      });
    }
  });

  socket.on('mute-user', async (data) => {
    try {
      const { roomId, targetUsername, action, reason, durationMinutes } = data; // action: 'mute' or 'unmute'

      if (!roomId || !targetUsername || !action) {
        console.log('❌ Invalid mute-user data:', data);
        socket.emit('mute-user-error', { error: 'Invalid mute data provided' });
        return;
      }

      // 1. SERVER-SIDE PERMISSION VERIFICATION - Use authoritative JWT data
      const hasMutePermission = await hasPermission(socket.userId, socket.username, roomId, 'mute');

      if (!hasMutePermission) {
        console.log(`🚫 User ${socket.username} (ID: ${socket.userId}) attempted to ${action} ${targetUsername} without permission in room ${roomId}`);
        socket.emit('mute-user-error', { 
          error: `You do not have permission to ${action} users in this room`,
          reason: 'no_permission'
        });
        return;
      }

      // 2. Get target user information from database (authoritative source)
      const targetUserResult = await pool.query('SELECT id, username, role FROM users WHERE username = $1', [targetUsername]);

      if (targetUserResult.rows.length === 0) {
        socket.emit('mute-user-error', { 
          error: 'Target user not found',
          reason: 'user_not_found'
        });
        return;
      }

      const targetUser = targetUserResult.rows[0];

      // 3. Prevent muting admins or yourself
      if (targetUser.id === socket.userId) {
        socket.emit('mute-user-error', { 
          error: 'You cannot mute yourself',
          reason: 'cannot_mute_self'
        });
        return;
      }

      if (targetUser.role === 'admin') {
        socket.emit('mute-user-error', { 
          error: 'Cannot mute administrators',
          reason: 'cannot_mute_admin'
        });
        return;
      }

      // 4. Notify the target user if they're online
      const targetUserSocket = [...connectedUsers.entries()].find(([socketId, userInfo]) => 
        userInfo.username === targetUsername && userInfo.roomId === roomId
      );

      if (targetUserSocket) {
        const [targetSocketId] = targetUserSocket;
        io.to(targetSocketId).emit('user-muted', {
          roomId,
          mutedUser: targetUsername,
          mutedBy: socket.username,
          action,
          reason: reason || 'No reason provided',
          duration: durationMinutes || null
        });
      }

      // 5. Broadcast verified mute event to room
      io.to(roomId).emit('user-muted', {
        roomId,
        mutedUser: targetUsername,
        mutedBy: socket.username, // Use server-side authoritative username
        action
      });

      // 6. Send verified system message
      const muteMessage = {
        id: Date.now().toString(),
        sender: 'System',
        content: `${targetUsername} was ${action}d by ${socket.username}${reason ? ` (${reason})` : ''}${durationMinutes ? ` for ${durationMinutes} minutes` : ''}`,
        timestamp: new Date().toISOString(),
        roomId: roomId,
        type: 'mute'
      };

      io.to(roomId).emit('new-message', muteMessage);

      // 7. Confirm success to requesting user
      socket.emit('mute-user-success', { action, targetUsername, roomId });

      console.log(`✅ User ${targetUsername} ${action}d in room ${roomId} by ${socket.username} (verified)`);

    } catch (error) {
      console.error('Error in mute-user handler:', error);
      socket.emit('mute-user-error', { 
        error: 'Internal server error',
        reason: 'server_error'
      });
    }
  });

  // Ban/unban user event - SECURE VERSION with server-side verification
  socket.on('ban-user', async (data) => {
    try {
      const { roomId, targetUsername, action, reason, durationHours } = data; // action: 'ban' or 'unban'

      if (!roomId || !targetUsername || !action) {
        console.log('❌ Invalid ban-user data:', data);
        socket.emit('ban-user-error', { error: 'Invalid ban data provided' });
        return;
      }

      // 1. SERVER-SIDE PERMISSION VERIFICATION - Use authoritative JWT data
      const hasBanPermission = await hasPermission(socket.userId, socket.username, roomId, 'ban');

      if (!hasBanPermission) {
        console.log(`🚫 User ${socket.username} (ID: ${socket.userId}) attempted to ${action} ${targetUsername} without permission in room ${roomId}`);
        socket.emit('ban-user-error', { 
          error: `You do not have permission to ${action} users in this room`,
          reason: 'no_permission'
        });
        return;
      }

      // 2. Get target user information from database (authoritative source)
      const targetUserResult = await pool.query('SELECT id, username FROM users WHERE username = $1', [targetUsername]);

      if (targetUserResult.rows.length === 0) {
        socket.emit('ban-user-error', { 
          error: 'Target user not found',
          reason: 'user_not_found'
        });
        return;
      }

      const targetUser = targetUserResult.rows[0];

      // 3. Prevent banning admins or room owners
      if (targetUser.id === socket.userId) {
        socket.emit('ban-user-error', { 
          error: 'You cannot ban yourself',
          reason: 'cannot_ban_self'
        });
        return;
      }

      // Check if target is admin
      const targetUserRoleResult = await pool.query('SELECT role FROM users WHERE id = $1', [targetUser.id]);
      if (targetUserRoleResult.rows.length > 0 && targetUserRoleResult.rows[0].role === 'admin') {
        socket.emit('ban-user-error', { 
          error: 'Cannot ban administrators',
          reason: 'cannot_ban_admin'
        });
        return;
      }

      if (action === 'ban') {
        // 4. PERSISTENT BAN STORAGE - Add to database
        const banResult = await addBanToDatabase(
          roomId, 
          targetUser.id, 
          targetUser.username, 
          socket.userId, 
          socket.username, 
          reason,
          durationHours
        );

        if (!banResult) {
          socket.emit('ban-user-error', { 
            error: 'Failed to ban user',
            reason: 'database_error'
          });
          return;
        }

        // 5. AUTHORITATIVE ENFORCEMENT - Remove from participants and disconnect if online
        if (roomParticipants[roomId]) {
          roomParticipants[roomId] = roomParticipants[roomId].filter(p => p.username !== targetUsername);
          io.to(roomId).emit('participants-updated', roomParticipants[roomId]);
        }

        // Force disconnect banned user from room if they're online
        const bannedUserSocket = [...connectedUsers.entries()].find(([socketId, userInfo]) => 
          userInfo.username === targetUsername && userInfo.roomId === roomId
        );

        if (bannedUserSocket) {
          const [bannedSocketId] = bannedUserSocket;
          io.to(bannedSocketId).emit('user-banned', {
            roomId,
            bannedUser: targetUsername,
            bannedBy: socket.username,
            action: 'ban',
            reason: reason || 'No reason provided',
            roomName: `Room ${roomId}`
          });

          // Force leave the room
          io.to(bannedSocketId).emit('force-leave-room', { 
            roomId, 
            reason: 'banned',
            bannedBy: socket.username 
          });
        }

        console.log(`✅ User ${targetUsername} banned from room ${roomId} by ${socket.username} (verified)`);

      } else if (action === 'unban') {
        // 6. PERSISTENT UNBAN - Remove from database
        const unbanResult = await removeBanFromDatabase(roomId, targetUsername, socket.userId, socket.username);

        if (!unbanResult) {
          socket.emit('ban-user-error', { 
            error: 'User was not banned or failed to unban',
            reason: 'not_banned_or_error'
          });
          return;
        }

        console.log(`✅ User ${targetUsername} unbanned from room ${roomId} by ${socket.username} (verified)`);
      }

      // 7. Broadcast verified ban/unban event to room
      io.to(roomId).emit('user-banned', {
        roomId,
        bannedUser: targetUsername,
        bannedBy: socket.username, // Use server-side authoritative username
        action,
        reason: reason || 'No reason provided',
        roomName: `Room ${roomId}`
      });

      // 8. Send verified system message
      const banMessage = {
        id: Date.now().toString(),
        sender: 'System',
        content: `${targetUsername} was ${action}ned by ${socket.username}${reason ? ` (${reason})` : ''}`,
        timestamp: new Date().toISOString(),
        roomId: roomId,
        type: action
      };

      io.to(roomId).emit('new-message', banMessage);

      // 9. Confirm success to requesting user
      socket.emit('ban-user-success', { action, targetUsername, roomId });

    } catch (error) {
      console.error('Error in ban-user handler:', error);
      socket.emit('ban-user-error', { 
        error: 'Internal server error',
        reason: 'server_error'
      });
    }
  });

  // Moderator events
  socket.on('moderator-added', async (data) => {
    const { roomId, username, roomName } = data;
    
    try {
      // Broadcast to all clients in room to update their moderator list
      io.to(roomId).emit('moderator-updated', {
        roomId: roomId,
        username: username,
        action: 'added'
      });
      
      console.log(`✅ Moderator ${username} added to room ${roomName} (${roomId})`);
    } catch (error) {
      console.error('Error in moderator-added handler:', error);
    }
  });

  socket.on('moderator-removed', async (data) => {
    const { roomId, username, roomName } = data;
    
    try {
      // Broadcast to all clients in room to update their moderator list
      io.to(roomId).emit('moderator-updated', {
        roomId: roomId,
        username: username,
        action: 'removed'
      });
      
      console.log(`✅ Moderator ${username} removed from room ${roomName} (${roomId})`);
    } catch (error) {
      console.error('Error in moderator-removed handler:', error);
    }
  });

  // Notification events
  socket.on('send-notification', (notificationData) => {
    const { targetUserId, targetUsername, notification } = notificationData;

    if (!targetUserId && !targetUsername) {
      console.log('Invalid notification data: no target specified');
      return;
    }

    // Find target user's socket
    let targetSocket = null;
    if (targetUsername) {
      targetSocket = [...connectedUsers.entries()].find(([socketId, userInfo]) => 
        userInfo.username === targetUsername
      );
    } else if (targetUserId) {
      targetSocket = [...connectedUsers.entries()].find(([socketId, userInfo]) => 
        userInfo.userId === targetUserId
      );
    }

    if (targetSocket) {
      const [targetSocketId] = targetSocket;
      io.to(targetSocketId).emit('new-notification', notification);

      // Special handling for coin notifications - show immediate alert
      if (notification.type === 'credit_received') {
        io.to(targetSocketId).emit('coin-received', {
          amount: notification.data?.amount || 0,
          from: notification.data?.from || 'Unknown',
          message: notification.message,
          timestamp: new Date().toISOString()
        });
      }

      console.log(`Notification sent to ${targetUsername || targetUserId}`);
    } else {
      console.log(`Target user ${targetUsername || targetUserId} not found for notification`);
    }
  });

  // Report event
  socket.on('send-report', (reportData) => {
    console.log('Gateway received report:', reportData);

    // Broadcast to admin users (you can filter by admin sockets)
    io.emit('admin-notification', {
      type: 'report',
      data: reportData,
      timestamp: new Date().toISOString()
    });

    console.log('Report forwarded to admins via gateway');
  });

  // Disconnect event
  socket.on('disconnect', async () => {
    console.log(`🔴 ===========================================`);
    console.log(`🔴 GATEWAY DISCONNECT!`);
    console.log(`❌ User disconnected from gateway: ${socket.id}`);
    console.log(`📊 Remaining connections: ${io.sockets.sockets.size - 1}`);
    console.log(`🔴 ===========================================`);

    const userInfo = connectedUsers.get(socket.id);
    if (userInfo && userInfo.roomId && userInfo.username) {
      // Check if this is a private chat room or support chat
      const isPrivateChat = userInfo.roomId.startsWith('private_');
      const isSupportChat = userInfo.roomId.startsWith('support_');

      // Check if user has other active connections (any room, any socket)
      const userAllConnections = [...connectedUsers.entries()].filter(([socketId, info]) => 
        socketId !== socket.id && 
        info.userId === userInfo.userId
      );

      // Check if user has other active connections in the same room
      const userOtherConnections = [...connectedUsers.entries()].filter(([socketId, info]) => 
        socketId !== socket.id && 
        info.username === userInfo.username && 
        info.roomId === userInfo.roomId
      );

      const hasOtherActiveConnections = userOtherConnections.length > 0;
      const hasAnyActiveConnections = userAllConnections.length > 0;

      // DO NOT auto-set status to offline on disconnect
      // Status is only set to offline via explicit logout endpoint
      // This keeps status (online/away/busy) persistent across socket reconnections
      if (!hasAnyActiveConnections && userInfo.userId) {
        console.log(`🔌 User ${userInfo.username} (ID: ${userInfo.userId}) has NO active connections - keeping current status (NOT auto-offline)`);
      } else if (hasAnyActiveConnections) {
        console.log(`🔌 User ${userInfo.username} still has ${userAllConnections.length} active connection(s) - keeping current status`);
      }

      // Remove from room participants only if no other connections exist
      if (roomParticipants[userInfo.roomId] && !hasOtherActiveConnections) {
        const participantBefore = roomParticipants[userInfo.roomId].find(p => p.userId === userInfo.userId);

        if (participantBefore) {
          // Remove participant completely from the room
          roomParticipants[userInfo.roomId] = roomParticipants[userInfo.roomId].filter(p => p.userId !== userInfo.userId);
          console.log(`🗑️ Removed ${userInfo.username} from room ${userInfo.roomId} participants (disconnect)`);

          // Sync participant count to database
          syncParticipantCountToDatabase(userInfo.roomId);

          // Notify room about updated participants
          io.to(userInfo.roomId).emit('participants-updated', roomParticipants[userInfo.roomId]);

          // Only broadcast leave message for public rooms (not private chats or support chats)
          // and ONLY if user has NO active connections anywhere (not just in this room)
          // This prevents "has left" message when user just switches apps or minimizes
          if (!isPrivateChat && !isSupportChat && !hasAnyActiveConnections) {
            // Check if we've already broadcast a leave message for this user in this room recently (within 5 seconds)
            const leaveKey = `${userInfo.userId}_${userInfo.roomId}`;
            const lastBroadcastTime = recentLeaveBroadcasts.get(leaveKey);
            const now = Date.now();
            const LEAVE_BROADCAST_COOLDOWN = 5000; // 5 seconds
            
            if (!lastBroadcastTime || (now - lastBroadcastTime) > LEAVE_BROADCAST_COOLDOWN) {
              // Use debounced broadcast to prevent spam from rapid reconnects
              scheduleBroadcast(io, 'leave', userInfo.userId, userInfo.roomId, userInfo.username, userInfo.role || 'user', socket);
              
              recentLeaveBroadcasts.set(leaveKey, now);
              
              // Clear announced join tracking when user truly leaves (so they can rejoin fresh)
              const joinKey = `${userInfo.userId}_${userInfo.roomId}`;
              announcedJoins.delete(joinKey);
              console.log(`🧹 Cleared join tracking for ${userInfo.username} in room ${userInfo.roomId}`);
              
              // Clean up old entries from recentLeaveBroadcasts (older than 10 seconds)
              for (const [key, timestamp] of recentLeaveBroadcasts.entries()) {
                if (now - timestamp > 10000) {
                  recentLeaveBroadcasts.delete(key);
                }
              }
              
              // Also clean up old announced joins (older than 1 hour)
              const ONE_HOUR = 60 * 60 * 1000;
              for (const [key, timestamp] of announcedJoins.entries()) {
                if (now - timestamp > ONE_HOUR) {
                  announcedJoins.delete(key);
                }
              }
            } else {
              console.log(`🔇 Skipping duplicate leave broadcast for ${userInfo.username} in room ${userInfo.roomId} (already broadcast ${Math.round((now - lastBroadcastTime) / 1000)}s ago)`);
            }
          } else {
            if (isPrivateChat) {
              console.log(`💬 Private chat disconnect - no broadcast for ${userInfo.username} in room ${userInfo.roomId}`);
            } else if (isSupportChat) {
              console.log(`🆘 Support chat disconnect - no broadcast for ${userInfo.username} in room ${userInfo.roomId}`);
            } else if (hasAnyActiveConnections) {
              console.log(`📱 User ${userInfo.username} still has active app connections - no leave broadcast (app backgrounded/switched)`);
            }
          }
        }
      } else if (hasOtherActiveConnections) {
        console.log(`🔄 User ${userInfo.username} has other active connections in room ${userInfo.roomId} - no leave broadcast`);
      } else {
        console.log(`👻 User ${userInfo.username} was not a participant in room ${userInfo.roomId} - no broadcast`);
      }
    }

    // MEMORY LEAK FIX: Clean up any pending broadcasts for this user
    if (userInfo && userInfo.userId && userInfo.roomId) {
      const broadcastKey = `${userInfo.userId}_${userInfo.roomId}`;
      const pending = pendingBroadcasts.get(broadcastKey);
      if (pending) {
        clearTimeout(pending.timeout);
        pendingBroadcasts.delete(broadcastKey);
        console.log(`🧹 Cleaned up pending broadcast for ${userInfo.username} in room ${userInfo.roomId}`);
      }
    }

    // Remove from connected users
    connectedUsers.delete(socket.id);
  });
});

// MEMORY LEAK FIX: Periodic cleanup for broadcast tracking Maps
setInterval(() => {
  const now = Date.now();
  let cleanedCount = 0;

  // Clean up recentBroadcasts (older than 30 seconds)
  for (const [key, timestamp] of recentBroadcasts.entries()) {
    if (now - timestamp > 30000) {
      recentBroadcasts.delete(key);
      cleanedCount++;
    }
  }

  // Clean up recentLeaveBroadcasts (older than 30 seconds)
  for (const [key, timestamp] of recentLeaveBroadcasts.entries()) {
    if (now - timestamp > 30000) {
      recentLeaveBroadcasts.delete(key);
      cleanedCount++;
    }
  }

  // Clean up announcedJoins (older than 2 hours)
  const TWO_HOURS = 2 * 60 * 60 * 1000;
  for (const [key, timestamp] of announcedJoins.entries()) {
    if (now - timestamp > TWO_HOURS) {
      announcedJoins.delete(key);
      cleanedCount++;
    }
  }

  if (cleanedCount > 0) {
    console.log(`🧹 Memory cleanup: removed ${cleanedCount} old broadcast tracking entries`);
  }
}, 60000); // Run every 1 minute

console.log('✅ Periodic memory cleanup scheduled (runs every minute)');

// Periodic cleanup job for inactive users (8-hour timeout)
const EIGHT_HOURS_MS = 8 * 60 * 60 * 1000; // 8 hours in milliseconds
const CLEANUP_INTERVAL_MS = 60 * 60 * 1000; // Run every 1 hour

setInterval(() => {
  console.log('🧹 Running inactivity cleanup job...');
  const now = Date.now();
  let removedCount = 0;

  // Check all rooms for inactive participants
  for (const [roomId, participants] of Object.entries(roomParticipants)) {
    const inactiveUsers = [];

    for (const participant of participants) {
      // Check if user has been inactive for 8 hours
      if (participant.lastActivityTime && (now - participant.lastActivityTime) >= EIGHT_HOURS_MS) {
        inactiveUsers.push(participant);
      }
    }

    // Remove inactive users from room
    for (const inactiveUser of inactiveUsers) {
      console.log(`⏰ Removing inactive user ${inactiveUser.username} from room ${roomId} (inactive for 8+ hours)`);

      // Remove from participants list
      roomParticipants[roomId] = roomParticipants[roomId].filter(p => p.userId !== inactiveUser.userId);
      removedCount++;

      // Broadcast leave message if it's a public room
      const isPrivateChat = roomId.startsWith('private_');
      const isSupportChat = roomId.startsWith('support_');

      if (!isPrivateChat && !isSupportChat) {
        const leaveMessage = {
          id: `leave_${Date.now()}_${inactiveUser.username}_${roomId}`,
          sender: inactiveUser.username,
          content: `${inactiveUser.username} was removed due to inactivity`,
          timestamp: new Date().toISOString(),
          roomId: roomId,
          type: 'leave',
          userRole: inactiveUser.role
        };

        io.to(roomId).emit('user-left', leaveMessage);
      }

      // Update participants list
      io.to(roomId).emit('participants-updated', roomParticipants[roomId]);

      // If user has an active socket, force them to leave the room
      if (inactiveUser.socketId) {
        const userSocket = io.sockets.sockets.get(inactiveUser.socketId);
        if (userSocket) {
          userSocket.leave(roomId);
          userSocket.emit('force-leave-room', {
            roomId,
            reason: 'inactivity',
            message: 'You were removed from the room due to 8 hours of inactivity'
          });
        }
      }
    }
  }

  if (removedCount > 0) {
    console.log(`✅ Inactivity cleanup complete: removed ${removedCount} inactive user(s)`);
  } else {
    console.log('✅ Inactivity cleanup complete: no inactive users found');
  }
}, CLEANUP_INTERVAL_MS);

console.log(`⏰ Inactivity cleanup job scheduled (runs every hour, 8-hour timeout)`);

// Red Packet expiry check (every minute)
setInterval(async () => {
  const expiredCount = await expireOldPackets();
  if (expiredCount > 0) {
    console.log(`🧧 Expired ${expiredCount} red packets and refunded unclaimed amounts`);
  }
}, 60000); // 1 minute

console.log(`🧧 Red packet expiry check scheduled (runs every minute)`);

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM received, shutting down Socket Gateway gracefully');
  server.close(() => {
    console.log('Socket Gateway terminated');
    process.exit(0);
  });
});

process.on('SIGINT', () => {
  console.log('SIGINT received, shutting down Socket Gateway gracefully');
  server.close(() => {
    console.log('Socket Gateway terminated');
    process.exit(0);
  });
});

// Start the gateway server
server.listen(GATEWAY_PORT, '0.0.0.0', () => {
  console.log(`✅ Socket Gateway running on port ${GATEWAY_PORT}`);
  console.log(`🌐 Gateway accessible at: http://0.0.0.0:${GATEWAY_PORT}`);
  console.log(`🔌 WebSocket endpoint: ws://0.0.0.0:${GATEWAY_PORT}`);
  console.log(`📡 Real-time features: Chat, Notifications, Typing indicators`);
  console.log(`🔐 JWT Authentication required for socket connections`);
  
  // Initialize Firebase for push notifications
  initializeFirebase();
}).on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    console.error(`❌ Port ${GATEWAY_PORT} is already in use`);
    process.exit(1);
  } else {
    console.error('Gateway server error:', err);
    process.exit(1);
  }
});

// roomParticipants accessible via HTTP endpoint /gateway/rooms/:roomId/participants


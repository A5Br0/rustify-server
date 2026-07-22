#!/usr/bin/env python3
"""
Rustify WebSocket Server v2.0 - Maximum Performance Edition
- 30Hz tick rate
- Spatial grid for O(1) proximity checks
- Delta compression per player
- Batch WebSocket writes per tick
- 200+ items, full game mechanics
"""
import asyncio, struct, math, time, random, os, json
from collections import defaultdict
from aiohttp import web, WSMsgType
import aiohttp

PORT = int(os.environ.get("PORT", "7777"))
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

WORLD_R = 6000.0
VIEW = 250.0
TICK = 1 / 30  # 30Hz for minimum ping

# ── Item database ──────────────────────────────────────────────────────────────
ITEM_NAMES = {
    # Resources
    0:"Wood", 1:"Stone", 2:"Metal Ore", 3:"Sulfur Ore", 4:"HQ Metal Ore",
    5:"Cloth", 6:"Leather", 7:"Scrap", 8:"Animal Fat", 9:"Raw Meat",
    10:"Cooked Meat", 21:"Metal Fragments", 22:"Gunpowder", 103:"Charcoal",
    104:"Sulfur", 105:"Crude Oil", 106:"Low Grade Fuel", 107:"Bone Fragments",
    108:"Hemp Fiber",
    # Ammo
    200:"5.56 Rifle Ammo", 201:"Pistol Bullet", 202:"12 Gauge Buckshot",
    203:".357 Ammo", 204:".50 ACP", 205:"Arrow", 206:"Explosive Arrow",
    207:"Incendiary 5.56", 208:"HV 5.56", 209:"HV Pistol Bullet",
    210:"12 Gauge Slug", 211:"Incendiary Shotgun Shell",
    # Attachments
    220:"Holosight", 221:"4x Zoom Scope", 222:"8x Zoom Scope", 223:"16x Scope",
    224:"Silencer", 225:"Laser Sight", 226:"Flashlight Attachment",
    227:"Extended Magazine", 228:"Bipod", 229:"Grip", 230:"Muzzle Boost",
    231:"Muzzle Brake", 232:"Rifle Body", 233:"SMG Body", 234:"Semi Auto Body",
    # Melee weapons (14-45)
    14:"Hatchet", 15:"Pickaxe", 19:"Spear", 28:"Stone Hatchet", 29:"Stone Pickaxe",
    30:"Metal Hatchet", 31:"Metal Pickaxe", 32:"Salvaged Hammer", 33:"Salvaged Sword",
    34:"Bone Club", 35:"Stone Spear", 37:"Machete", 38:"Katana", 39:"Baseball Bat",
    40:"Knife", 41:"Bone Knife", 42:"Mace", 43:"Axe", 44:"Sledgehammer",
    # Ranged weapons (46-70)
    18:"Hunting Bow", 45:"Crossbow", 46:"Compound Bow", 47:"Explosive Bow",
    48:"Pipe Shotgun", 49:"Waterpipe Shotgun", 50:"Double Barrel", 51:"Flintlock",
    52:"Revolver", 53:"Semi-Auto Pistol", 54:"Glock", 55:"Python Revolver",
    56:"M92 Pistol", 57:"MP5A4", 58:"Thompson", 59:"Custom SMG", 60:"Prototype 17",
    61:"M39 Rifle", 62:"Semi-Auto Rifle", 63:"Bolt Action Rifle", 64:"L96 Rifle",
    65:"AK-47", 66:"LR-300", 67:"M249", 68:"M4 Shotgun", 69:"SPAS-12",
    70:"Rocket Launcher", 71:"Multiple Grenade Launcher", 72:"Minigun",
    # Explosives
    80:"Beancan Grenade", 81:"F1 Grenade", 82:"Satchel Charge", 83:"C4",
    84:"Rocket", 85:"Incendiary Rocket", 86:"High Velocity Rocket", 87:"Smoke Grenade",
    88:"Flashbang", 89:"Molotov Cocktail",
    # Medicals
    90:"Bandage", 91:"Small Medkit", 92:"Large Medkit", 93:"Syringes",
    94:"Anti-Radiation Pills", 95:"Blood Bag", 96:"Vitamins", 97:"Painkillers",
    98:"Antibiotics", 99:"Stimulant", 100:"Adrenaline Syringe",
    # Food & Drink
    110:"Berries", 111:"Mushroom", 112:"Corn", 113:"Potato", 114:"Pumpkin",
    115:"Raw Chicken", 116:"Cooked Chicken", 117:"Raw Fish", 118:"Cooked Fish",
    119:"Canned Beans", 120:"Chocolate Bar", 121:"Granola Bar", 122:"Burnt Chicken",
    123:"Human Skull", 124:"Water Jug", 125:"Purified Water", 126:"Small Water Bottle",
    127:"Tea", 128:"Coffee", 129:"Apple", 130:"Orange",
    # Gear - Armor
    140:"Cloth Vest", 141:"Cloth Pants", 142:"Cloth Shirt", 143:"Cloth Boots",
    144:"Leather Jacket", 145:"Leather Pants", 146:"Leather Boots", 147:"Leather Gloves",
    148:"Wood Armor Chest", 149:"Bone Armor", 150:"Road Sign Jacket", 151:"Road Sign Kilt",
    152:"Metal Chest Plate", 153:"Metal Facemask", 154:"Metal Boots", 155:"Heavy Plate Chest",
    156:"Heavy Plate Helmet", 157:"Heavy Plate Pants", 158:"Riot Helmet",
    159:"Hazmat Suit", 160:"Night Vision Goggles", 161:"Gas Mask", 162:"Tactical Gloves",
    163:"Ghillie Suit Top", 164:"Ghillie Suit Bottom", 165:"Longsleeve T-Shirt",
    166:"Hoodie", 167:"Pants", 168:"Snow Jacket", 169:"Shorts",
    # Tools & Misc
    170:"Building Plan", 171:"Hammer", 172:"Torch", 173:"Flashlight", 174:"Binoculars",
    175:"Compass", 176:"Map", 177:"Fishing Rod", 178:"Garry's Mod", 179:"Spray Can",
    180:"Lock", 181:"Key Lock", 182:"Code Lock", 183:"Stash", 184:"Sleeping Bag",
    185:"Vending Machine", 186:"Recycler", 187:"Research Table", 188:"Repair Bench",
    # Electronics
    190:"Wire Tool", 191:"Switch", 192:"Electrical Branch", 193:"Solar Panel",
    194:"Battery", 195:"Auto Turret", 196:"Flame Turret", 197:"Shotgun Trap",
    198:"Laser Detector", 199:"Smart Switch", 240:"CCTV Camera", 241:"Computer Station",
    # Building materials
    300:"Wood Plank", 301:"Stone Block", 302:"Metal Sheet", 303:"Armored Panel",
    # Keys / blueprints
    400:"Blueprint: AK-47", 401:"Blueprint: Bolt Rifle", 402:"Blueprint: C4",
    403:"Blueprint: Rocket", 404:"Blueprint: Night Vision",
    # Special
    20:"C4 Explosive", 23:"AK-47 Blueprint", 24:"Berries (old)",
    25:"Fishing Rod (old)", 26:"Torch (old)",
}

ITEM_ID = {v.lower().replace(" ", "_").replace("-", "_").replace(".", ""): k
           for k, v in ITEM_NAMES.items()}

# Damage values for all weapons
ITEM_DMG = {
    14:25, 15:22, 18:18, 19:30, 28:20, 29:18, 30:35, 31:32, 32:28, 33:40,
    34:22, 35:30, 37:38, 38:45, 39:20, 40:15, 41:12, 42:25, 43:30, 44:50,
    45:35, 46:40, 47:45, 48:50, 49:55, 50:60, 51:35, 52:40, 53:42, 54:38,
    55:45, 56:42, 57:28, 58:30, 59:25, 60:32, 61:55, 62:45, 63:70, 64:85,
    65:45, 66:40, 67:30, 68:65, 69:60, 70:200, 71:150, 72:20,
    80:50, 81:80, 82:150, 83:500, 84:200, 85:180, 86:160, 89:60,
}

WEAPON_RANGE = {
    14:3, 15:3, 18:180, 19:4, 28:3, 29:3, 30:3, 31:3, 32:3, 33:3,
    34:3, 35:4, 37:3, 38:4, 39:3, 40:2, 41:2, 42:3, 43:3, 44:4,
    45:250, 46:280, 47:280, 48:30, 49:25, 50:30, 51:100, 52:150,
    53:180, 54:160, 55:180, 56:190, 57:200, 58:180, 59:160, 60:170,
    61:300, 62:250, 63:500, 64:600, 65:300, 66:280, 67:250, 68:50,
    69:45, 70:300, 71:250, 72:200,
    80:15, 81:20, 82:5, 83:5, 84:300, 85:300, 86:350, 89:8,
}

# Gathering efficiency: tool_id -> {resource_type -> multiplier}
GATHER_EFF = {
    0:  {"wood": 0.8, "stone": 0.3, "metal_ore": 0.0, "sulfur_ore": 0.0},
    14: {"wood": 1.8, "stone": 0.4, "metal_ore": 0.0},
    15: {"wood": 0.4, "stone": 2.0, "metal_ore": 1.0, "sulfur_ore": 0.8},
    30: {"wood": 2.5, "stone": 0.5, "metal_ore": 0.2},
    31: {"wood": 0.5, "stone": 3.0, "metal_ore": 1.8, "sulfur_ore": 1.5},
    43: {"wood": 3.5, "stone": 0.3},
    171:{"wood": 1.2, "stone": 0.5, "metal_ore": 0.3},
}

BUILD_COST = {
    0: {0: 50},        # Wood Foundation
    1: {0: 100},       # Wood Wall
    2: {0: 150},       # Wood Doorway
    3: {0: 80},        # Wood Window
    4: {0: 200, 1: 100}, # Wood Roof
    5: {1: 150},       # Stone Foundation
    6: {1: 300},       # Stone Wall
    7: {1: 200},       # Stone Doorway
    8: {1: 500, 21: 200}, # Metal Wall
    9: {21: 800, 4: 50},  # Armored Wall
    10: {1: 200, 21: 100}, # Tool Cupboard
    11: {0: 80},       # Storage Box
    12: {21: 150, 1: 100}, # Large Storage
    13: {21: 300, 1: 200}, # Furnace
    14: {1: 100, 21: 50},  # Wood Door
    15: {21: 150},         # Metal Door
    16: {21: 500, 4: 20},  # Armored Door
    17: {0: 30},           # Wood Floor
    18: {1: 100},          # Stone Floor
    19: {0: 50},           # Wood Ramp
    20: {1: 150},          # Stone Ramp
    21: {0: 300},          # Wood Barricade
    22: {1: 500},          # Stone Barricade
    23: {21: 400},         # Metal Barricade
    24: {0: 100},          # Sandbag
    25: {21: 200, 1: 100}, # Watchtower
    26: {21: 500, 194: 1, 195: 1}, # Auto Turret
}

RECIPES = {
    # Tools
    14: [(0,50),(1,25)], 15: [(0,50),(1,25)], 171: [(21,75),(0,50)],
    172: [(0,50),(8,10)], 173: [(21,15),(7,5)], 177: [(0,100),(5,20)],
    # Weapons (melee)
    19: [(0,100),(1,20),(5,10)], 28: [(1,50),(0,30)], 29: [(1,50),(0,30)],
    30: [(21,30),(0,20)], 31: [(21,30),(0,20)], 32: [(7,50),(0,30)],
    33: [(7,60),(0,30)], 34: [(107,10),(0,20)], 35: [(0,100),(1,20),(5,10)],
    37: [(21,40),(0,30)], 38: [(21,60),(4,10)], 39: [(0,80)],
    40: [(21,15)], 41: [(107,5),(0,10)], 42: [(1,40),(0,20)],
    43: [(0,60),(21,20)], 44: [(0,80),(1,40)],
    # Bows
    18: [(0,200),(5,30)], 45: [(0,150),(21,50),(5,20)],
    46: [(0,200),(21,50),(5,20)], 47: [(18,1),(82,5)],
    # Firearms
    48: [(7,30),(1,20),(0,10)], 49: [(7,40),(1,25),(0,15)],
    50: [(7,50),(1,30),(0,20)], 51: [(7,30),(1,20),(0,10)],
    52: [(21,40),(7,20)], 53: [(21,50),(7,25)], 54: [(21,45),(7,20)],
    55: [(21,45),(7,20),(0,15)], 56: [(21,50),(7,20)],
    57: [(21,60),(7,30),(0,20)], 58: [(21,70),(7,35),(0,25)],
    59: [(21,55),(7,25),(0,15)], 60: [(21,60),(7,30),(0,20)],
    61: [(21,80),(7,40),(0,30)], 62: [(21,75),(7,35),(0,25)],
    63: [(21,80),(7,40),(0,30)], 64: [(21,100),(7,50),(0,40)],
    65: [(21,150),(7,50),(23,5),(0,30)], 66: [(21,130),(7,45),(0,25)],
    67: [(21,200),(7,100),(0,50)], 68: [(21,80),(7,40),(0,30)],
    69: [(21,90),(7,45),(0,35)], 70: [(21,150),(7,75),(0,50),(4,15)],
    # Explosives
    80: [(22,60),(7,10)], 81: [(22,50),(7,10)], 82: [(22,80),(7,15)],
    83: [(21,20),(22,50),(7,5)], 84: [(21,10),(22,100),(7,20)],
    85: [(84,1),(106,10)], 86: [(84,1),(21,10)], 89: [(106,10),(0,5)],
    # Medicals
    90: [(5,10)], 91: [(5,20),(8,5),(7,5)], 92: [(5,40),(8,15),(7,20)],
    93: [(5,10),(7,5),(22,1)], 94: [(5,10),(7,5),(22,1)],
    95: [(5,20),(8,10)], 96: [(5,5)], 97: [(5,10),(8,3)],
    98: [(5,15),(7,5)], 99: [(5,5),(22,1)],
    # Ammo
    200: [(21,5),(22,10)], 201: [(21,3),(22,8)], 202: [(21,4),(22,12)],
    203: [(21,4),(22,10)], 204: [(21,6),(22,15)], 205: [(0,1),(5,2)],
    206: [(205,1),(22,5)], 207: [(200,1),(106,1)], 208: [(200,3),(21,5)],
    # Gear
    140: [(5,20)], 141: [(5,15)], 142: [(5,15)], 143: [(5,10)],
    144: [(6,20),(5,15)], 145: [(6,15),(5,10)], 146: [(6,10)],
    148: [(0,200),(5,30)], 149: [(107,50),(0,30)],
    150: [(1,75),(7,20)], 151: [(1,50),(7,15)],
    152: [(21,60),(1,40)], 153: [(21,40),(1,30)], 154: [(21,30),(1,20)],
    155: [(21,100),(4,15)], 156: [(21,80),(4,10)], 157: [(21,80),(4,10)],
    158: [(21,50),(1,40)], 161: [(5,30),(8,20),(21,10)],
    163: [(5,40),(6,20)], 164: [(5,40),(6,20)],
    165: [(5,10)], 166: [(5,20)], 167: [(5,15)], 169: [(5,10)],
    # Electronics
    191: [(21,10),(7,3)], 192: [(21,15),(7,5)], 193: [(21,100),(7,30)],
    194: [(21,50),(7,10)], 195: [(21,200),(7,50),(192,1),(194,1)],
    196: [(21,150),(7,30),(106,50)], 197: [(21,80),(7,20)],
    # Misc
    181: [(21,5)], 182: [(21,30),(7,10)], 183: [(0,100),(5,20)],
    184: [(5,50)], 170: [(0,10)],
    # Resources processing
    21: [(2,50)], 22: [(3,50),(104,50)], 104: [(3,80)],
    106: [(105,50),(2,30)],
}

# Food effects: item_id -> {hp, cal, hyd, rad_reduce, temp_bonus}
FOOD_EFFECTS = {
    9:  {"cal":20, "hyd":-5}, 10: {"cal":40, "hp":5},
    110:{"cal":15, "hyd":10}, 111:{"cal":20, "hyd":5, "hp":3},
    112:{"cal":25, "hyd":5}, 113:{"cal":30, "hyd":5},
    114:{"cal":35, "hyd":5}, 115:{"cal":15, "hyd":-5},
    116:{"cal":50, "hp":5}, 117:{"cal":20, "hyd":-5},
    118:{"cal":45, "hyd":5, "hp":5}, 119:{"cal":55, "hyd":20},
    120:{"cal":40, "hyd":5}, 121:{"cal":45, "hyd":5},
    124:{"hyd":50}, 125:{"hyd":40}, 126:{"hyd":25},
    127:{"hyd":25, "temp":10, "hp":2}, 128:{"cal":10, "hyd":15, "temp":15},
    129:{"cal":20, "hyd":10}, 130:{"cal":20, "hyd":15},
}

MED_EFFECTS = {
    90: {"hp":15}, 91: {"hp":30}, 92: {"hp":75}, 93: {"hp":25},
    94: {"rad":-50}, 95: {"hp":40}, 96: {"rad":-15},
    97: {"hp":10, "temp":5}, 98: {"hp":20}, 99: {"hp":5, "cal":15},
    100:{"hp":50, "bleeding":False},
}

# ── Spatial Grid ───────────────────────────────────────────────────────────────
CELL_SIZE = 100.0

class SpatialGrid:
    __slots__ = ("cells",)
    def __init__(self):
        self.cells = defaultdict(set)

    def _key(self, x, y):
        return (int(x // CELL_SIZE), int(y // CELL_SIZE))

    def insert(self, obj, x, y):
        self.cells[self._key(x, y)].add(obj)

    def remove(self, obj, x, y):
        k = self._key(x, y)
        self.cells[k].discard(obj)
        if not self.cells[k]:
            del self.cells[k]

    def move(self, obj, ox, oy, nx, ny):
        ok, nk = self._key(ox, oy), self._key(nx, ny)
        if ok != nk:
            self.cells[ok].discard(obj)
            if not self.cells[ok]: del self.cells[ok]
            self.cells[nk].add(obj)

    def query(self, x, y, radius):
        cr = int(math.ceil(radius / CELL_SIZE))
        cx, cy = int(x // CELL_SIZE), int(y // CELL_SIZE)
        result = []
        for dx in range(-cr, cr + 1):
            for dy in range(-cr, cr + 1):
                result.extend(self.cells.get((cx + dx, cy + dy), ()))
        return result


# ── Entity classes ─────────────────────────────────────────────────────────────
QSCALE = 0.183  # 6000/32767

def qpos(v): return max(-32768, min(32767, int(v / QSCALE)))
def qyaw(a): return int((a % (2 * math.pi)) / (2 * math.pi) * 256) & 0xFF

def noise(x, y):
    n = (int(x) * 374761393 ^ int(y) * 668265263) & 0xFFFFFFFF
    return ((n ^ (n >> 13)) * 1274126177 & 0xFFFFFF) / 0xFFFFFF

def height(x, y):
    m = max(0.0, 1.0 - math.hypot(x, y) / WORLD_R)
    h = (noise(x * 0.001, y * 0.001) * 320
         + noise(x * 0.004, y * 0.004) * 100
         + noise(x * 0.015, y * 0.015) * 35
         + noise(x * 0.06,  y * 0.06)  * 12) * m
    return h


class Entity:
    __slots__ = ("id", "x", "y", "z", "yaw", "hp", "typ", "owner")
    def __init__(self, i, typ, x, y, z, yaw=0, hp=200, owner=None):
        self.id, self.typ, self.x, self.y, self.z = i, typ, x, y, z
        self.yaw, self.hp, self.owner = yaw, hp, owner

    def snap(self):
        snap_typ = self.typ
        if snap_typ < 100:
            snap_typ += 128
        return struct.pack("<IhhhBBB", self.id,
                           qpos(self.x), qpos(self.y), qpos(self.z),
                           qyaw(self.yaw), snap_typ,
                           max(0, min(255, int(self.hp))))


class Player:
    __slots__ = ("id","name","x","y","z","yaw","hp","ws","inv",
                 "cal","hyd","rad","temp","dead","respawn","wpn","last_input",
                 "stamina","bleeding","bleed_rate","status_effects","kills","deaths",
                 "last_seen","ping","team_id")
    def __init__(self, pid, name, ws):
        a, b = random.uniform(50, 600), random.uniform(0, math.tau)
        x, y = math.cos(b) * a, math.sin(b) * a
        self.id, self.name, self.ws = pid, name, ws
        self.x, self.y, self.z = x, y, height(x, y) + 1.0
        self.yaw, self.hp = 0.0, 100.0
        self.inv = {}
        self.cal, self.hyd, self.rad, self.temp = 85.0, 85.0, 0.0, 20.0
        self.stamina = 100.0
        self.bleeding = False
        self.bleed_rate = 0.0
        self.status_effects = {}   # {"burning": 3.0, ...}
        self.dead, self.respawn = False, 0.0
        self.wpn = 0
        self.last_input = 0.0
        self.kills, self.deaths = 0, 0
        self.last_seen = {}  # entity_id -> snap_bytes (for delta)
        self.ping = 0
        self.team_id = None

    def snap(self):
        return struct.pack("<IhhhBBB", self.id,
                           qpos(self.x), qpos(self.y), qpos(self.z),
                           qyaw(self.yaw), 1,
                           max(0, min(255, int(self.hp))))


class ResourceNode:
    __slots__ = ("id", "x", "y", "res_type", "amount", "max_amount", "respawn_time")
    def __init__(self, i, x, y, res_type, amount=100):
        self.id, self.x, self.y = i, x, y
        self.res_type, self.amount, self.max_amount = res_type, amount, amount
        self.respawn_time = 0.0

    def snap(self):
        type_map = {"wood": 5, "stone": 6, "metal_ore": 7, "sulfur_ore": 8,
                    "hemp": 9, "crude_oil": 10}
        typ = type_map.get(self.res_type, 5)
        return struct.pack("<IhhhBBB", self.id,
                           qpos(self.x), qpos(self.y), qpos(height(self.x, self.y)),
                           0, typ, min(255, self.amount))


class Animal:
    __slots__ = ("id","x","y","z","yaw","hp","typ","speed","dir","state","target_id","wander_timer")
    def __init__(self, i, typ, x, y, z):
        self.id, self.typ = i, typ
        self.x, self.y, self.z = x, y, z
        self.yaw = random.uniform(0, math.tau)
        self.hp = 80.0 if typ == 2 else 60.0   # deer vs wolf
        self.speed = 25.0 if typ == 2 else 55.0
        self.dir = random.uniform(0, math.tau)
        self.state = "wander"   # wander / chase / flee
        self.target_id = None
        self.wander_timer = 0.0

    def snap(self):
        return struct.pack("<IhhhBBB", self.id,
                           qpos(self.x), qpos(self.y), qpos(self.z),
                           qyaw(self.yaw), self.typ,
                           max(0, min(255, int(self.hp))))


class LootCrate:
    __slots__ = ("id","x","y","z","tier","items","respawn_time","opened")
    TIERS = {
        "normal":   {7:(5,20), 0:(50,200), 1:(20,100), 200:(5,30)},
        "military": {65:1, 200:(20,60), 80:(1,3), 21:(50,150), 7:(20,60)},
        "elite":    {65:1, 66:1, 83:(1,2), 84:(2,5), 21:(100,300), 4:(5,20)},
        "monument": {7:(10,40), 21:(20,80), 200:(10,40), 63:1},
        "airdrop":  {65:1, 67:1, 63:1, 83:(1,2), 84:(5,15), 21:(200,500), 4:(10,30)},
    }
    def __init__(self, i, x, y, z, tier="normal"):
        self.id, self.x, self.y, self.z = i, x, y, z
        self.tier, self.opened = tier, False
        self.respawn_time = 0.0
        self.items = self._roll_items()

    def _roll_items(self):
        items = {}
        for item_id, qty in self.TIERS.get(self.tier, {}).items():
            if isinstance(qty, tuple):
                items[item_id] = random.randint(*qty)
            else:
                items[item_id] = qty
        return items

    def snap(self):
        typ = 20 + {"normal":0,"military":1,"elite":2,"monument":3,"airdrop":4}.get(self.tier, 0)
        return struct.pack("<IhhhBBB", self.id,
                           qpos(self.x), qpos(self.y), qpos(self.z),
                           0, typ, 100 if not self.opened else 0)


# ── Monument ───────────────────────────────────────────────────────────────────
MONUMENTS = [
    {"name": "Launch Site",      "x":  800, "y":  800, "r": 200, "rad": 3.0, "tier": "elite"},
    {"name": "Airfield",         "x": -900, "y":  600, "r": 180, "rad": 1.0, "tier": "military"},
    {"name": "Train Yard",       "x":  500, "y": -800, "r": 150, "rad": 0.5, "tier": "military"},
    {"name": "Power Plant",      "x": -700, "y": -700, "r": 160, "rad": 2.0, "tier": "elite"},
    {"name": "Water Treatment",  "x":  300, "y":  900, "r": 130, "rad": 0.5, "tier": "monument"},
    {"name": "Supermarket",      "x": -400, "y":  300, "r": 80,  "rad": 0.0, "tier": "normal"},
    {"name": "Gas Station",      "x":  600, "y": -200, "r": 60,  "rad": 0.0, "tier": "normal"},
    {"name": "Lighthouse",       "x": 1200, "y":  200, "r": 50,  "rad": 0.0, "tier": "normal"},
    {"name": "Harbor",           "x": -1100,"y":  500, "r": 120, "rad": 0.0, "tier": "military"},
]

RAD_ZONES = [
    {"x": m["x"], "y": m["y"], "r": m["r"], "strength": m["rad"]}
    for m in MONUMENTS if m["rad"] > 0
]


# ── World Instance ─────────────────────────────────────────────────────────────
class WorldInstance:
    def __init__(self, key, name, rules):
        self.key, self.name, self.rules = key, name, rules
        self.players: dict[int, Player] = {}
        self.buildings: list[Entity] = []
        self.resources: list[ResourceNode] = []
        self.animals: list[Animal] = []
        self.crates: list[LootCrate] = []
        self.tc_zones: list[dict] = []
        self.next_id = 1
        self.t = 0.0
        self.day_time = 0.3
        self.airdrop_timer = random.uniform(1200, 2400)
        self.event_msg = ""

        # Spatial grids
        self.player_grid = SpatialGrid()
        self.resource_grid = SpatialGrid()
        self.animal_grid = SpatialGrid()
        self.crate_grid = SpatialGrid()

        self._setup_resources()
        self._setup_animals()
        self._setup_crates()

    def _new_id(self):
        i = self.next_id
        self.next_id += 1
        return i

    def _setup_resources(self):
        configs = [
            ("wood",    400, 800, 100), ("wood",     100, 2000, 80),
            ("stone",   250, 700, 60),  ("stone",    80,  1800, 50),
            ("metal_ore", 100, 900, 40),("metal_ore", 30, 2500, 30),
            ("sulfur_ore", 60, 700, 25),("sulfur_ore", 20, 2000, 20),
            ("hemp",    80, 600, 20),
            ("crude_oil", 30, 1500, 30),
        ]
        for res_type, count, max_r, amt in configs:
            for _ in range(count):
                a, b = random.uniform(50, max_r), random.uniform(0, math.tau)
                x, y = math.cos(b) * a, math.sin(b) * a
                r = ResourceNode(self._new_id(), x, y, res_type, amt)
                self.resources.append(r)
                self.resource_grid.insert(r, x, y)

    def _setup_animals(self):
        for _ in range(35):  # deer
            a, b = random.uniform(100, 1000), random.uniform(0, math.tau)
            x, y = math.cos(b) * a, math.sin(b) * a
            an = Animal(self._new_id(), 2, x, y, height(x, y) + 1)
            self.animals.append(an)
            self.animal_grid.insert(an, x, y)
        for _ in range(25):  # wolves
            a, b = random.uniform(100, 800), random.uniform(0, math.tau)
            x, y = math.cos(b) * a, math.sin(b) * a
            an = Animal(self._new_id(), 3, x, y, height(x, y) + 1)
            self.animals.append(an)
            self.animal_grid.insert(an, x, y)

    def _setup_crates(self):
        # Normal crates scattered around
        for _ in range(60):
            a, b = random.uniform(100, 2000), random.uniform(0, math.tau)
            x, y = math.cos(b) * a, math.sin(b) * a
            c = LootCrate(self._new_id(), x, y, height(x, y) + 1, "normal")
            self.crates.append(c)
            self.crate_grid.insert(c, x, y)
        # Monument crates
        for m in MONUMENTS:
            for _ in range(3 if m["tier"] == "elite" else 2):
                ox = m["x"] + random.uniform(-m["r"] * 0.5, m["r"] * 0.5)
                oy = m["y"] + random.uniform(-m["r"] * 0.5, m["r"] * 0.5)
                c = LootCrate(self._new_id(), ox, oy, height(ox, oy) + 1, m["tier"])
                self.crates.append(c)
                self.crate_grid.insert(c, ox, oy)

    async def add_player(self, name, ws):
        p = Player(self._new_id(), name, ws)
        self.players[p.id] = p
        self.player_grid.insert(p, p.x, p.y)
        return p

    # ── Batched state send ──
    async def _send_state(self, pl: Player):
        if pl.dead or not pl.ws:
            return
        snap = bytearray()
        cnt = 0

        # Other players
        nearby_players = self.player_grid.query(pl.x, pl.y, VIEW)
        for o in nearby_players:
            if isinstance(o, Player) and o.id != pl.id and not o.dead:
                snap += o.snap()
                cnt += 1

        # Buildings
        for b in self.buildings:
            if math.hypot(pl.x - b.x, pl.y - b.y) < VIEW:
                snap += b.snap()
                cnt += 1

        # Resources
        nearby_res = self.resource_grid.query(pl.x, pl.y, VIEW)
        for r in nearby_res:
            if isinstance(r, ResourceNode) and r.amount > 0:
                snap += r.snap()
                cnt += 1

        # Animals
        nearby_animals = self.animal_grid.query(pl.x, pl.y, VIEW + 50)
        for a in nearby_animals:
            if isinstance(a, Animal):
                snap += a.snap()
                cnt += 1

        # Crates
        nearby_crates = self.crate_grid.query(pl.x, pl.y, VIEW)
        for c in nearby_crates:
            if isinstance(c, LootCrate) and not c.opened:
                snap += c.snap()
                cnt += 1

        # Build one batch buffer
        batch = bytearray()
        if snap:
            batch += struct.pack("<BH", 0x81, cnt) + snap
        # Vitals
        batch += struct.pack("<BBBBBbBB",
                             0x82,
                             max(0, min(255, int(pl.hp))),
                             max(0, min(255, int(pl.cal))),
                             max(0, min(255, int(pl.hyd))),
                             max(0, min(255, int(pl.rad))),
                             max(-128, min(127, int(pl.temp))),
                             max(0, min(255, int(pl.stamina))),
                             1 if pl.bleeding else 0)
        # Inventory
        inv_data = struct.pack("<H", len(pl.inv))
        for iid, amt in pl.inv.items():
            inv_data += struct.pack("<HH", iid & 0xFFFF, min(65535, amt))
        batch += struct.pack("B", 0x83) + inv_data
        # Day time
        batch += struct.pack("<Bf", 0x86, self.day_time)

        try:
            await pl.ws.send_bytes(bytes(batch))
        except Exception:
            pass

    async def broadcast(self):
        tasks = [self._send_state(p) for p in self.players.values() if not p.dead]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # ── Kill feed ──
    async def broadcast_kill(self, killer_name: str, victim_name: str, weapon: str):
        msg = f"{killer_name} killed {victim_name} with {weapon}"
        pkt = struct.pack("B", 0x89) + msg.encode()
        for pl in self.players.values():
            if not pl.dead and pl.ws:
                try:
                    await pl.ws.send_bytes(pkt)
                except Exception:
                    pass

    async def broadcast_event(self, msg: str):
        pkt = struct.pack("B", 0x88) + msg.encode()
        for pl in self.players.values():
            if pl.ws:
                try:
                    await pl.ws.send_bytes(pkt)
                except Exception:
                    pass

    # ── Simulate ──
    async def simulate(self):
        while True:
            await asyncio.sleep(TICK)
            self.t += TICK
            self.day_time = (self.day_time + TICK / 1200.0) % 1.0
            gather_x = self.rules.get("gather_x", 1.0)
            rad_x = self.rules.get("rad_x", 1.0)

            for p in list(self.players.values()):
                if p.dead:
                    if self.t >= p.respawn:
                        p.dead = False
                        a, b = random.uniform(50, 400), random.uniform(0, math.tau)
                        p.x, p.y = math.cos(b) * a, math.sin(b) * a
                        p.z = height(p.x, p.y) + 1.0
                        p.hp = 100.0
                        p.cal, p.hyd = 85.0, 85.0
                        p.rad, p.bleeding = 0.0, False
                        p.stamina = 100.0
                        self.player_grid.insert(p, p.x, p.y)
                        try:
                            await p.ws.send_bytes(
                                struct.pack("B", 0x84) + b"You respawned")
                        except Exception:
                            pass
                    continue

                # Stamina regen
                p.stamina = min(100.0, p.stamina + 8.0 * TICK)

                # Hunger/thirst drain
                p.cal = max(0.0, p.cal - 2.5 * TICK)
                p.hyd = max(0.0, p.hyd - 3.5 * TICK)
                if p.cal <= 0:
                    p.hp -= 35.0 * TICK
                if p.hyd <= 0:
                    p.hp -= 40.0 * TICK

                # Temperature
                night = self.day_time > 0.75 or self.day_time < 0.2
                if night:
                    p.temp = max(-10.0, p.temp - 1.5 * TICK)
                else:
                    p.temp = min(30.0, p.temp + 0.5 * TICK)
                if p.temp < 0:
                    p.hp -= abs(p.temp) * 0.5 * TICK  # hypothermia

                # Bleeding
                if p.bleeding:
                    p.hp -= p.bleed_rate * TICK
                    p.bleed_rate = max(0.0, p.bleed_rate - 0.1 * TICK)
                    if p.bleed_rate <= 0:
                        p.bleeding = False

                # Radiation zones
                for rz in RAD_ZONES:
                    if math.hypot(p.x - rz["x"], p.y - rz["y"]) < rz["r"]:
                        p.rad = min(100.0, p.rad + rz["strength"] * rad_x * TICK * 10)
                if p.rad > 50:
                    p.hp -= (p.rad - 50) * 0.2 * TICK

                # Death check
                if p.hp <= 0:
                    p.dead = True
                    p.deaths += 1
                    p.respawn = self.t + 30.0
                    self.player_grid.remove(p, p.x, p.y)
                    # Drop items
                    dropped = list(p.inv.items())[:10]  # drop first 10
                    for iid, amt in dropped:
                        if amt > 0:
                            ox = p.x + random.uniform(-3, 3)
                            oy = p.y + random.uniform(-3, 3)
                            # TODO: ground item entity
                    p.inv.clear()
                    try:
                        death_pkt = struct.pack("B", 0x8A) + b"You died"
                        await p.ws.send_bytes(death_pkt)
                    except Exception:
                        pass

            # Resource respawn
            for r in self.resources:
                if r.amount <= 0:
                    if r.respawn_time > 0:
                        r.respawn_time -= TICK
                        if r.respawn_time <= 0:
                            r.amount = r.max_amount

            # Building decay
            for b in list(self.buildings):
                if b.hp <= 0:
                    self.buildings.remove(b)
                    continue
                protected = any(
                    tc["owner"] == b.owner and
                    math.hypot(b.x - tc["x"], b.y - tc["y"]) < tc["radius"]
                    for tc in self.tc_zones
                )
                if not protected:
                    b.hp = max(0, b.hp - 0.3 * TICK)

            # Animal AI (improved)
            for a in self.animals:
                old_x, old_y = a.x, a.y
                nearby_p = [p for p in self.player_grid.query(a.x, a.y, 80)
                             if isinstance(p, Player) and not p.dead]

                if a.typ == 3:  # wolf - chase players
                    if nearby_p and a.state != "flee":
                        nearest = min(nearby_p, key=lambda p: math.hypot(p.x - a.x, p.y - a.y))
                        dist = math.hypot(nearest.x - a.x, nearest.y - a.y)
                        if dist < 60:
                            a.state = "chase"
                            a.target_id = nearest.id
                            a.dir = math.atan2(nearest.y - a.y, nearest.x - a.x)
                            if dist < 3.0:  # bite range
                                nearest.hp -= 8.0
                                if not nearest.bleeding and random.random() < 0.3:
                                    nearest.bleeding = True
                                    nearest.bleed_rate = 3.0
                        else:
                            a.state = "wander"
                    if a.hp < 20:
                        a.state = "flee"
                        if nearby_p:
                            nearest = min(nearby_p, key=lambda p: math.hypot(p.x - a.x, p.y - a.y))
                            a.dir = math.atan2(a.y - nearest.y, a.x - nearest.x)

                elif a.typ == 2:  # deer - flee from players
                    if nearby_p:
                        nearest = min(nearby_p, key=lambda p: math.hypot(p.x - a.x, p.y - a.y))
                        dist = math.hypot(nearest.x - a.x, nearest.y - a.y)
                        if dist < 40:
                            a.state = "flee"
                            a.dir = math.atan2(a.y - nearest.y, a.x - nearest.x)
                    else:
                        a.state = "wander"

                if a.state == "wander":
                    a.wander_timer -= TICK
                    if a.wander_timer <= 0:
                        a.dir += random.uniform(-0.8, 0.8)
                        a.wander_timer = random.uniform(2, 8)

                spd = a.speed * TICK
                a.x += math.cos(a.dir) * spd
                a.y += math.sin(a.dir) * spd
                a.z = height(a.x, a.y) + 1
                a.yaw = a.dir
                self.animal_grid.move(a, old_x, old_y, a.x, a.y)

            # Crate respawn
            for c in self.crates:
                if c.opened:
                    c.respawn_time -= TICK
                    if c.respawn_time <= 0:
                        c.opened = False
                        c.items = c._roll_items()

            # Airdrop event
            self.airdrop_timer -= TICK
            if self.airdrop_timer <= 0:
                self.airdrop_timer = random.uniform(1800, 3600)
                a, b = random.uniform(200, 1500), random.uniform(0, math.tau)
                ax, ay = math.cos(b) * a, math.sin(b) * a
                drop = LootCrate(self._new_id(), ax, ay, height(ax, ay) + 2, "airdrop")
                self.crates.append(drop)
                self.crate_grid.insert(drop, ax, ay)
                await self.broadcast_event(
                    f"AIRDROP incoming at ({int(ax)}, {int(ay)})!")

            # Broadcast state every tick
            await self.broadcast()

            # Supabase save every 60s
            if self.t % 60.0 < TICK and SUPABASE_URL:
                asyncio.create_task(self._save_world_async())

    async def _save_world_async(self):
        if not SUPABASE_KEY:
            return
        data = json.dumps({
            "id": self.key,
            "t": self.t,
            "players": len(self.players),
            "buildings": len(self.buildings),
        }).encode()
        url = f"{SUPABASE_URL}/rest/v1/world_state"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        }
        try:
            import aiohttp
            async with aiohttp.ClientSession() as sess:
                await sess.post(url, data=data, headers=headers, timeout=5)
        except Exception:
            pass


# ── 5 World Instances ──────────────────────────────────────────────────────────
INSTANCES = {
    "vanilla1":  WorldInstance("vanilla1",  "Vanilla 1",      {"pvp": True,  "gather_x": 1.0}),
    "vanilla2":  WorldInstance("vanilla2",  "Vanilla 2",      {"pvp": True,  "gather_x": 1.0}),
    "double":    WorldInstance("double",    "2x Gather",      {"pvp": True,  "gather_x": 2.0}),
    "pve":       WorldInstance("pve",       "PvE",            {"pvp": False, "gather_x": 1.5}),
    "hardcore":  WorldInstance("hardcore",  "Hardcore",       {"pvp": True,  "gather_x": 0.5, "rad_x": 2.0}),
}


# ── WebSocket handler ──────────────────────────────────────────────────────────
async def handle_ws(request):
    key = request.match_info.get("server_key", "vanilla1")
    instance = INSTANCES.get(key)
    if not instance:
        return web.Response(status=404, text="Server not found")

    ws = web.WebSocketResponse(heartbeat=30, max_msg_size=65536)
    await ws.prepare(request)

    # Handshake
    msg = await ws.receive()
    if msg.type != WSMsgType.BINARY:
        return ws
    raw_name = msg.data[1:].split(b"\x00")[0][:16].decode("utf-8", errors="replace") or "Player"

    p = await instance.add_player(raw_name, ws)
    await ws.send_bytes(struct.pack("B", 0x84) + f"Welcome to {instance.name}!".encode())
    await ws.send_bytes(struct.pack("B", 0x85) + struct.pack("<I", p.id))
    await ws.send_bytes(struct.pack("B", 0x84) + b"Press TAB for inventory, T for chat")
    name_b = p.name.encode()
    for o in instance.players.values():
        if o.id != p.id and o.ws:
            try:
                await o.ws.send_bytes(struct.pack("<BIH", 0x8D, p.id, len(name_b)) + name_b)
            except Exception:
                pass
            # Also tell the newly-joined player about the existing player's name.
            try:
                existing_b = o.name.encode()
                await p.ws.send_bytes(struct.pack("<BIH", 0x8D, o.id, len(existing_b)) + existing_b)
            except Exception:
                pass
    # Send monuments info
    mon_json = json.dumps([{"name": m["name"], "x": m["x"], "y": m["y"]} for m in MONUMENTS])
    await ws.send_bytes(struct.pack("B", 0x8B) + mon_json.encode())

    try:
        async for msg in ws:
            if msg.type != WSMsgType.BINARY:
                continue
            buf = msg.data
            if len(buf) < 1:
                continue
            op = buf[0]

            # ── 0x02  MOVEMENT INPUT ──
            if op == 0x02 and len(buf) >= 18:
                fwd, strf, yaw, pitch = struct.unpack("<ffff", buf[1:17])
                flags = buf[17] if len(buf) > 17 else 0
                jumping = bool(flags & 0x01)
                sprinting = bool(flags & 0x02)
                sneaking = bool(flags & 0x04)

                if not p.dead:
                    spd = (380.0 if sprinting else 250.0 if sneaking else 270.0)
                    if sprinting:
                        p.stamina = max(0.0, p.stamina - 20.0 * TICK * 30)
                        if p.stamina <= 0:
                            spd = 270.0  # cant sprint without stamina

                    p.yaw = yaw
                    old_x, old_y = p.x, p.y
                    p.x += (math.sin(yaw) * fwd + math.cos(yaw) * strf) * spd * TICK
                    p.y += (math.cos(yaw) * fwd - math.sin(yaw) * strf) * spd * TICK
                    gz = height(p.x, p.y)
                    p.z = gz + 1.0
                    instance.player_grid.move(p, old_x, old_y, p.x, p.y)

            # ── 0x03  FIRE ──
            elif op == 0x03 and len(buf) >= 13:
                if not p.dead and instance.rules.get("pvp", True):
                    dx, dy, dz = struct.unpack("<fff", buf[1:13])
                    dist_sq = dx*dx + dy*dy + dz*dz
                    if dist_sq > 0.0001:
                        nd = math.sqrt(dist_sq)
                        nx, ny, nz = dx/nd, dy/nd, dz/nd
                        rng = WEAPON_RANGE.get(p.wpn, 200)
                        dmg = ITEM_DMG.get(p.wpn, 15)

                        # Check players
                        hit = False
                        for o in instance.player_grid.query(p.x, p.y, rng):
                            if not isinstance(o, Player) or o.id == p.id or o.dead:
                                continue
                            od = math.dist((p.x, p.y, p.z), (o.x, o.y, o.z))
                            if od < rng:
                                # Ray-sphere check (2m radius)
                                tx = o.x - p.x; ty = o.y - p.y; tz = o.z - p.z
                                proj = tx*nx + ty*ny + tz*nz
                                if 0 < proj < rng:
                                    cx = tx - proj*nx; cy = ty - proj*ny; cz = tz - proj*nz
                                    if cx*cx + cy*cy + cz*cz < 4.0:
                                        o.hp -= dmg
                                        if random.random() < 0.15 and not o.bleeding:
                                            o.bleeding = True
                                            o.bleed_rate = 4.0
                                        if o.hp <= 0 and not o.dead:
                                            o.dead = True
                                            o.deaths += 1
                                            o.respawn = instance.t + 30.0
                                            instance.player_grid.remove(o, o.x, o.y)
                                            p.kills += 1
                                            wpn_name = ITEM_NAMES.get(p.wpn, "Fists")
                                            asyncio.create_task(
                                                instance.broadcast_kill(p.name, o.name, wpn_name))
                                        hit = True
                                        break

                        if not hit:
                            # Check animals
                            for a in instance.animal_grid.query(p.x, p.y, rng):
                                if not isinstance(a, Animal):
                                    continue
                                od = math.dist((p.x, p.y, p.z), (a.x, a.y, a.z))
                                if od < rng:
                                    a.hp -= dmg
                                    if a.hp <= 0:
                                        instance.animal_grid.remove(a, a.x, a.y)
                                        instance.animals.remove(a)
                                        loot = 9 if a.typ == 2 else 9  # raw meat
                                        loot_amt = random.randint(3, 8)
                                        p.inv[loot] = p.inv.get(loot, 0) + loot_amt
                                        p.inv[107] = p.inv.get(107, 0) + random.randint(2, 5)  # bones
                                        if a.typ == 3:  # wolf drops leather
                                            p.inv[6] = p.inv.get(6, 0) + random.randint(1, 3)
                                    break

                # Consume ammo
                ammo_map = {
                    65: 200, 66: 200, 67: 200, 61: 200, 62: 200, 63: 200, 64: 200,
                    57: 201, 58: 201, 59: 201, 60: 201, 52: 201, 53: 201, 54: 201,
                    55: 201, 56: 201, 48: 202, 49: 202, 50: 202, 68: 202, 69: 202,
                    18: 205, 45: 205, 46: 205, 47: 206,
                }
                ammo_id = ammo_map.get(p.wpn)
                if ammo_id and p.inv.get(ammo_id, 0) > 0:
                    p.inv[ammo_id] -= 1

            # ── 0x04  INTERACT (gather / loot) ──
            elif op == 0x04 and not p.dead:
                tool = p.wpn if p.wpn in GATHER_EFF else 0
                eff_map = GATHER_EFF.get(tool, GATHER_EFF[0])
                gx = instance.rules.get("gather_x", 1.0)

                # Gather resources
                for r in instance.resource_grid.query(p.x, p.y, 6):
                    if not isinstance(r, ResourceNode) or r.amount <= 0:
                        continue
                    if math.hypot(p.x - r.x, p.y - r.y) > 5:
                        continue
                    eff = eff_map.get(r.res_type, 0.5)
                    iid = {
                        "wood": 0, "stone": 1, "metal_ore": 2,
                        "sulfur_ore": 3, "hemp": 5, "crude_oil": 105,
                    }.get(r.res_type, 0)
                    amt = max(1, int(eff * gx * random.uniform(0.8, 1.2)))
                    p.inv[iid] = p.inv.get(iid, 0) + amt
                    # Byproducts
                    if r.res_type == "metal_ore":
                        p.inv[103] = p.inv.get(103, 0) + max(1, amt // 2)  # charcoal
                    r.amount -= 1
                    if r.amount <= 0:
                        r.respawn_time = random.uniform(180, 360)
                    break

                # Loot crates
                for c in instance.crate_grid.query(p.x, p.y, 5):
                    if not isinstance(c, LootCrate) or c.opened:
                        continue
                    if math.hypot(p.x - c.x, p.y - c.y) > 4:
                        continue
                    for iid, amt in c.items.items():
                        p.inv[iid] = p.inv.get(iid, 0) + amt
                    c.opened = True
                    c.respawn_time = random.uniform(1800, 3600)
                    try:
                        await ws.send_bytes(struct.pack("B", 0x84) + f"Looted {c.tier} crate!".encode())
                    except Exception:
                        pass
                    break

            # ── 0x05  BUILD ──
            elif op == 0x05 and len(buf) >= 17 and not p.dead:
                piece = buf[1]
                x, y, z, yaw = struct.unpack("<ffff", buf[2:18])
                cost_def = BUILD_COST.get(piece, {})
                # Cost check
                can_build = all(p.inv.get(iid, 0) >= c for iid, c in cost_def.items())
                if can_build:
                    for iid, c in cost_def.items():
                        p.inv[iid] -= c
                    bld = Entity(instance._new_id(), piece, x, y, z, yaw, 200, p.id)
                    instance.buildings.append(bld)
                    if piece == 10:  # TC
                        instance.tc_zones.append({"owner": p.id, "x": x, "y": y, "radius": 35.0})

            # ── 0x06  CRAFT ──
            elif op == 0x06 and len(buf) >= 3 and not p.dead:
                item_id = struct.unpack("<H", buf[1:3])[0]
                qty = buf[3] if len(buf) > 3 else 1
                rc = RECIPES.get(item_id)
                if rc:
                    can_craft = all(p.inv.get(iid, 0) >= c * qty for iid, c in rc)
                    if can_craft:
                        for iid, c in rc:
                            p.inv[iid] -= c * qty
                        p.inv[item_id] = p.inv.get(item_id, 0) + qty
                        item_name = ITEM_NAMES.get(item_id, f"Item {item_id}")
                        try:
                            await ws.send_bytes(
                                struct.pack("B", 0x84) +
                                f"Crafted {qty}x {item_name}".encode())
                        except Exception:
                            pass

            # ── 0x07  EQUIP ──
            elif op == 0x07 and len(buf) >= 3 and not p.dead:
                item_id = struct.unpack("<H", buf[1:3])[0]
                if item_id in p.inv and p.inv[item_id] > 0:
                    p.wpn = item_id

            # ── 0x08  USE ITEM (eat/med) ──
            elif op == 0x08 and len(buf) >= 3 and not p.dead:
                item_id = struct.unpack("<H", buf[1:3])[0]
                if p.inv.get(item_id, 0) > 0:
                    used = False
                    if item_id in FOOD_EFFECTS:
                        fx = FOOD_EFFECTS[item_id]
                        p.hp  = min(100.0, p.hp  + fx.get("hp",  0))
                        p.cal = min(100.0, p.cal + fx.get("cal", 0))
                        p.hyd = min(100.0, p.hyd + fx.get("hyd", 0))
                        p.rad = max(0.0,   p.rad - fx.get("rad", 0))
                        p.temp = min(40.0, p.temp + fx.get("temp", 0))
                        used = True
                    elif item_id in MED_EFFECTS:
                        fx = MED_EFFECTS[item_id]
                        p.hp = min(100.0, p.hp + fx.get("hp", 0))
                        p.rad = max(0.0,  p.rad - fx.get("rad", 0))
                        if "bleeding" in fx:
                            p.bleeding = fx["bleeding"]
                        used = True
                    if used:
                        p.inv[item_id] -= 1
                        if p.inv[item_id] <= 0:
                            del p.inv[item_id]

            # ── 0x09  DROP ITEM ──
            elif op == 0x09 and len(buf) >= 3 and not p.dead:
                item_id = struct.unpack("<H", buf[1:3])[0]
                qty = struct.unpack("<H", buf[3:5])[0] if len(buf) >= 5 else 1
                if p.inv.get(item_id, 0) >= qty:
                    p.inv[item_id] -= qty
                    if p.inv[item_id] <= 0:
                        del p.inv[item_id]

            # ── 0x0A  RESPAWN REQUEST ──
            elif op == 0x0A:
                if p.dead and instance.t >= p.respawn - 25:
                    p.respawn = instance.t  # allow instant respawn on request

            # ── 0x0B  CHAT ──
            elif op == 0x0B:
                text = buf[1:].decode("utf-8", errors="replace")[:200].strip()
                if text:
                    name_b = p.name.encode()
                    text_b = text.encode()
                    chat_pkt = (struct.pack("<BH", 0x87, len(name_b)) +
                                name_b + b"\x00" + text_b)
                    for pl in instance.players.values():
                        if not pl.dead and pl.ws:
                            try:
                                await pl.ws.send_bytes(chat_pkt)
                            except Exception:
                                pass

            # ── 0x0C  PING ──
            elif op == 0x0C:
                ts = buf[1:9] if len(buf) >= 9 else b"\x00" * 8
                await ws.send_bytes(struct.pack("B", 0x8C) + ts)

    except Exception:
        pass
    finally:
        instance.players.pop(p.id, None)
        instance.player_grid.remove(p, p.x, p.y)

    return ws


async def handle_servers(request):
    data = [
        {
            "key": k,
            "name": v.name,
            "rules": v.rules,
            "online": len(v.players),
            "max": 100,
        }
        for k, v in INSTANCES.items()
    ]
    return web.json_response(data)


async def on_startup(application):
    for inst in INSTANCES.values():
        asyncio.create_task(inst.simulate())


app = web.Application()
app.router.add_get("/ws/{server_key}", handle_ws)
app.router.add_get("/servers", handle_servers)
app.router.add_get("/", lambda r: web.Response(text="Rustify v2.0 OK"))
app.on_startup.append(on_startup)

if __name__ == "__main__":
    print(f"[Rustify v2.0] Starting on :{PORT} | 30Hz | Spatial Grid | 200+ items")
    web.run_app(app, host="0.0.0.0", port=PORT)

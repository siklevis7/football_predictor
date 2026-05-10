
-- ── Venues (coordinates for weather lookup) ───────────────────────────────────
CREATE TABLE IF NOT EXISTS venues (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(100) NOT NULL UNIQUE,
    city        VARCHAR(100),
    latitude    DECIMAL(9,6),
    longitude   DECIMAL(9,6),
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_name (name)
);

-- Pre-load known PL venues
INSERT IGNORE INTO venues (name, city, latitude, longitude) VALUES
('Emirates Stadium',          'London',      51.5550, -0.1084),
('Anfield',                   'Liverpool',   53.4308, -2.9608),
('Old Trafford',              'Manchester',  53.4631, -2.2913),
('Etihad Stadium',            'Manchester',  53.4831, -2.2004),
('Stamford Bridge',           'London',      51.4816, -0.1910),
('Tottenham Hotspur Stadium', 'London',      51.6042, -0.0666),
('St. James Park',            'Newcastle',   54.9754, -1.6218),
('Villa Park',                'Birmingham',  52.5092, -1.8847),
('Goodison Park',             'Liverpool',   53.4388, -2.9666),
('Amex Stadium',              'Brighton',    50.8609, -0.0832),
('Molineux Stadium',          'Wolverhampton',52.5902,-2.1302),
('London Stadium',            'London',      51.5386, -0.0162),
('Selhurst Park',             'London',      51.3983, -0.0855),
('Gtech Community Stadium',   'London',      51.4882, -0.2866),
('Craven Cottage',            'London',      51.4749, -0.2218),
('City Ground',               'Nottingham',  52.9399, -1.1323),
('Vitality Stadium',          'Bournemouth', 50.7352, -1.8382),
('Bramall Lane',              'Sheffield',   53.3703, -1.4706),
('King Power Stadium',        'Leicester',   52.6204, -1.1424),
('Portman Road',              'Ipswich',     52.0545,  1.1446),
('St Marys Stadium',          'Southampton', 50.9058, -1.3914),
('Stadium of Light',          'Sunderland',  54.9147, -1.3883),
('Kenilworth Road',           'Luton',       51.8839, -0.4317),
('Turf Moor',                 'Burnley',     53.7889, -2.2302);

SELECT 'Venues table ready.' AS status;

-- ── Add advanced stats columns to matches_stats ─────────────────────────────
-- Run this if matches_stats table already exists
ALTER TABLE matches_stats
  ADD COLUMN IF NOT EXISTS npxg              FLOAT,
  ADD COLUMN IF NOT EXISTS shot_quality      FLOAT,
  ADD COLUMN IF NOT EXISTS penalties_awarded INT DEFAULT 0;

SELECT 'Advanced stats columns added.' AS status;

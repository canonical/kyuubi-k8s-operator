CREATE TABLE kyuubi_users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL,
    salt VARCHAR(100) NOT NULL,
    password_hash VARCHAR(255) NOT NULL
);

DO $$
DECLARE
    salt TEXT := gen_salt('bf');
BEGIN
    INSERT INTO kyuubi_users (username, salt, password_hash) VALUES ('admin', salt, crypt('admin_password', salt));
END $$;
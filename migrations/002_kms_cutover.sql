DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'gym_qr_root_keys'
          AND column_name = 'vault_ciphertext'
    ) THEN
        ALTER TABLE gym_qr_root_keys RENAME COLUMN vault_ciphertext TO key_ciphertext;
        ALTER TABLE gym_qr_root_keys RENAME COLUMN vault_key_reference TO key_reference;

        IF EXISTS (SELECT 1 FROM gym_qr_root_keys LIMIT 1) THEN
            RAISE EXCEPTION 'KMS cutover requires empty gym_qr_root_keys; legacy Transit ciphertext cannot be decrypted by KMS';
        END IF;
    END IF;
END $$;

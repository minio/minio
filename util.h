#include <stdint.h>

int bcrypt_pbkdf(const char *pass, size_t pass_len, const uint8_t *salt,
    size_t salt_len, uint8_t *key, size_t key_len, unsigned int rounds);
void explicit_bzero(void *b, size_t len);

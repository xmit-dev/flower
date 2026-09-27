/* Flower extension translation unit. Upstream remains byte-for-byte pinned.
 * Private property inspection lets the value codec and the canonical JSON fast
 * path read application data without executing getters or other hooks.
 * SPDX-License-Identifier: MIT */
#include "upstream/quickjs.c"
#include "json-check.c"
#include "canonical-json.c"
#include "wire.c"
#include "crypto-view.c"

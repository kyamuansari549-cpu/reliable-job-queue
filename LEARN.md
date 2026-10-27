# Reliable Job Queue - Seekho Samjho (Hinglish Guide)

Ye file unke liye hai jo is project ko baad me aaram se, bilkul
shuruaat se samajhna chahte hain. Koi jaldi nahi - step by step chalo.

---

## 1. Job Queue hota kya hai?

Socho tum ek restaurant me gaye. Tum order dete ho, counter wala ek
token number deta hai, aur tum baith ke wait karte ho. Kitchen andar
apni speed se order banata rehta hai - tumhe counter pe khade rehna
nahi padta.

Software me bhi bilkul yahi hota hai:

- Kuch kaam **slow** hote hain - email bhejna, report banana, photo
  process karna, payment gateway ko call karna.
- Agar user ko ye sab hone tak wait karana pade, to app slow lagega
  aur user bhaag jayega.
- Isliye app bolta hai: "Request mil gayi!" (token mil gaya) - aur
  asal kaam **background me** ek **queue** (line) me lag jata hai.
- Alag se chalne wale **workers** line se ek-ek job uthate hain aur
  pura karte hain.

Is project ka kaam bas yahi hai: jobs ko line me lagana, workers se
karwana, aur agar kuch gadbad ho to sambhal ke rakhna.

**Job** = ek kaam ki puri detail: kya karna hai (`type`), kis data ke
saath (`payload`), kitni baar try karna hai (`max_retries`).

**Queue** = jobs ki line, database (SQLite) me stored - taaki server
restart ho jaye to bhi jobs gayab na hon.

**Worker** = background thread jo line se job uthata hai aur uska
handler (function) chalata hai.

## 2. Problem: job fail ho jaye to?

Real life me failures normal hain:

- Email server 2 minute down hai.
- Internet flicker ho gaya.
- Dusri service ne timeout de diya.

Agar hum fail hote hi job phenk dein, to user ka email kabhi nahi
jayega. Isliye samajhdari ye hai: **thodi der ruk ke dobara try karo.**
Isko **retry** kehte hain.

Lekin retry ki bhi limit honi chahiye. Agar koi job hamesha hi fail
ho raha hai (jaise galat email format jo kabhi sahi nahi hoga), to
use hamesha retry karna time aur bijli ki barbadi hai.

Isi liye do concepts aate hain - **backoff** aur **DLQ**.

## 3. Exponential Backoff - "ruk, saans le, phir try kar"

Agar koi service down hai aur hum har 1 second me try karte rahen,
to hum use aur dabaa denge. Backoff ka matlab: har failure ke baad
**wait time dugna** kar do.

Is project ka formula:

```
wait = backoff_base * 2^(attempts - 1)
```

`backoff_base` default 1 second hai:

| Failure # | attempts | Wait |
|-----------|----------|------|
| 1st fail  | 1        | 1s   |
| 2nd fail  | 2        | 2s   |
| 3rd fail  | 3        | 4s   |
| 4th fail  | 4        | 8s   |

Dekho pattern: 1, 2, 4, 8 - har baar dugna. Isko hi **exponential**
kehte hain. Fayda: chhoti gadbad (2 second ki) turant theek ho jati
hai, aur lambi gadbad pe system shaant rehta hai, zor nahi lagata.

Job table me `run_at` field yahi decide karta hai ke job **kab**
dobara due hoga. Worker sirf wahi jobs uthata hai jinka
`run_at <= abhi ka time` hai.

## 4. DLQ (Dead-Letter Queue) - "ye wala sach me mar gaya"

Jab job ke saare retries khatm ho jayein (`attempts > max_retries`),
hum use delete **nahi** karte. Use `dead` status de ke ek alag line
me rakh dete hain - yahi **Dead-Letter Queue** hai.

Kyun delete nahi karte?

1. **Saboot rehta hai** - kya fail hua, kyun fail hua (`last_error`
   me pura error likha hota hai).
2. **Paise/data ka nuksan nahi** - socho ek payment job dead ho gaya
   aur humne delete kar diya. Customer ka paisa kata, order kabhi
   nahi bana, aur koi record bhi nahi. DLQ me hota to pata chal jata.
3. **Fix ke baad dobara chance** - bug fix karke deploy kar do, phir
   dead job ko **requeue** kar do (attempts wapas 0, status `queued`)
   aur wo normal tareeke se process ho jata hai.

Real companies (Amazon SQS, Google Pub/Sub, RabbitMQ) sab me DLQ
hota hai. Ye is project ka sabse "professional" feature hai.

Ek khaas case: agar job ka `type` hi galat hai - koi handler hi
register nahi hai us type ke liye - to retry karna bekaar hai, wo
kabhi succeed nahi hoga. Aise job ko hum **turant dead** kar dete
hain, saaf error ke saath.

## 5. Dusre zaroori concepts

**Priority** - Sab jobs barabar nahi hote. Password-reset email,
newsletter se zyada urgent hai. `priority` zyada = line me aage.
Worker hamesha sabse zyada priority wala due job pehle uthata hai
(ek hi priority ho to purana wala pehle - FIFO).

**Idempotency** - User ne double-click kar diya, ya network retry ne
ek hi request do baar bhej di. Agar dono baar job ban gaya to email
do baar jayega - bura lagega. Iska ilaaj: enqueue karte time ek
`idempotency_key` do (jaise order id). Database me ye key **unique**
hai - dusri baar wahi key aayi to naya job nahi banta, purana wala
hi wapas mil jata hai.

**Atomic claim** - Do workers ek saath khaali baithe hain, line me
ek hi job hai. Agar dono ne ek hi job utha liya to email do baar
jayega. Is project me claim ek `BEGIN IMMEDIATE` transaction ke
andar hota hai - SQLite pure database ko lock kar deta hai us pal
ke liye, to dusra worker tab tak rukta hai. Aur upar se Python ka
lock bhi hai. Result: **ek job, ek hi worker, pakka.**

**Persistence** - Jobs RAM me nahi, SQLite **file** me rehte hain.
Server band karke dobara chalao, queued jobs wahi ke wahi milenge.
Test `test_persistence_across_reopen` yahi prove karta hai.

## 6. File-by-file samjho

### `job_queue.py` - dimag (core engine)

API (FastAPI) se alag, shudh Python engine. Isi liye isko bina web
server ke bhi test aur use kar sakte ho.

**`JobQueue.__init__(db_path, backoff_base, now_fn)`**
- Database file kholta hai, `jobs` table banata hai (agar pehle se
  hai to wahi use hota hai).
- `now_fn` ek function hai jo "abhi ka time" deta hai. Default
  `time.time` hai, lekin tests me hum **nakli ghadi** (FakeClock)
  dete hain - taaki 4 second ke backoff ke liye sach me 4 second
  rukna na pade. Time aage badhana humare haath me hota hai. Isko
  **dependency injection** kehte hain.
- Ek `threading.RLock` - jab kai threads ek saath database chhoote
  hain to ye lock line banata hai, gadbad nahi hoti.

**`register_handler(type, fn)`**
- Ek dictionary me `type -> function` jodta hai. Jaise
  `"send_email" -> send_email function`.
- Handler ko do cheezein milti hain: `payload` (job ka data) aur
  `job` (puri job ki detail - attempts wagera). Handler jo return
  karega wo job ka `result` ban jayega. Handler **exception** fekega
  to attempt fail mana jayega. Bas - handler ke liye yahi contract hai.

**`enqueue(type, payload, priority, max_retries, delay_seconds, idempotency_key)`**
- Nayi row insert karta hai: status `queued`, attempts 0,
  `run_at = abhi + delay_seconds` (delay 0 matlab turant due).
- Pehle idempotency key check karta hai - purana job hai to wahi
  return, koi duplicate nahi.
- Poora job (dict banke) return karta hai.

**`claim_next()`**
- Sabse critical function. Ek transaction me: sabse zyada priority
  wala due job dhoondo (`status='queued' AND run_at <= now`), use
  `running` mark karo, commit karo.
- Transaction ki wajah se do claim ek saath nahi takrate.

**`process_next()`**
- `claim_next()` + handler chalana, ek package me. Worker aur tests
  isi ko baar-baar bulate hain.

**`_execute(job)`** (private - underscore se shuru)
- Handler nahi mila -> turant `dead`, saaf error ke saath.
- Handler chala aur return aaya -> status `succeeded`, result save.
- Exception aaya -> `_handle_failure` ko de diya.

**`_handle_failure(job, exc)`**
- `attempts + 1` karta hai.
- `attempts > max_retries` -> status `dead`, error save. (DLQ!)
- Warna backoff formula se naya `run_at` set karke wapas `queued`.

**`requeue(job_id)`**
- Sirf `dead` job pe chalta hai (aur kisi pe chalao to ValueError -
  galti pakdi jati hai).
- `attempts = 0`, `run_at = abhi`, error saaf, status `queued`.

**`stats()` / `list_jobs(status)` / `get_job(id)`**
- Seedhe reads: status ke hisaab se ginati, list, ek job.

**`Worker` class**
- `run_once()` = ek `process_next()`. Tests ke liye.
- `run_forever(stop_event, poll_interval)` = loop: job hai to
  process karo, nahi hai to thodi der so jao (poll interval), jab
  tak rukne ka signal na mile.

### `main.py` - darwaza (FastAPI service)

**Env config** - `JOB_DB_PATH`, `JOB_BACKOFF_BASE`,
`JOB_WORKER_COUNT`, `JOB_POLL_INTERVAL` environment se padhta hai.
Galat value (jaise number ki jagah text) pe saaf error ke saath
band hota hai - chup-chaap galat config pe chalna khatarnak hota hai.

**Demo handlers**
- `send_email` - nakli email: kuch bhejna nahi, bas
  `{"sent": True, ...}` return karta hai.
- `generate_report` - payload ka chhota summary return karta hai.
- `flaky_task` - **`fail_times` baar fail, phir succeed.** Ye retries
  demo karne ke liye hai. Dhyaan do: iski ginati job ke `attempts`
  se nahi, apni alag dictionary se hoti hai jo **job id** pe
  keyed hai. Kyun? Kyunki requeue `attempts` ko 0 kar deta hai -
  aur hum chahte hain ke DLQ se wapas aane ke baad flaky apni ginati
  yaad rakhe, taaki wo "theek ho gaya" wala behave kare.
- `always_fail` - hamesha exception. DLQ demo ke liye.

**`lifespan`** - FastAPI ka startup/shutdown hook. App chalu hote
hi worker threads shuru, app band hote hi pehle stop signal, phir
threads ka join (graceful shutdown - beech me kaam adhura nahi
chhodte).

**Endpoints**
- `POST /jobs` -> 201 ke saath naya job. Galat input pe 400.
- `GET /jobs`, `GET /jobs?status=dead` -> list, filter ke saath.
- `GET /jobs/{id}` -> ek job; na mile to 404.
- `GET /dlq` -> sirf dead jobs (DLQ ka darwaza).
- `POST /jobs/{id}/requeue` -> dead job ko dobara line me.
- `GET /stats`, `GET /health`.

### `tests/test_job_queue.py`

Test do hisso me hain:

- **Core tests** - FakeClock ke saath, ek-ek behaviour pakka wala:
  succeed hona, exact `max_retries` ke baad dead hona, backoff ka
  1-2-4 pattern **number se verify**, flaky ka recover hona, requeue
  ke baad success, priority ki ordering, idempotency (do baar
  enqueue = ek job), persistence (band karke khola, job waisa hi),
  aur 4 threads ke saath daud - koi job do baar claim nahi hua.
- **API tests** - asal app, asal background worker, sirf database
  temporary file me. Email job succeed, `always_fail` DLQ me,
  `/dlq` aur `/stats` consistent, dead flaky job requeue karke
  succeed - sab end-to-end.

`drain()` helper samajh lo: worker ko tab tak chalata hai jab tak
line khaali na ho, aur beech me jab sab jobs future me scheduled
hon to FakeClock ko seedha unke `run_at` tak aage badha deta hai.
Isiliye 8 second ke backoff wala test bhi millisecond me khatm
ho jata hai.

## 7. Khud try karo (exercises)

1. **Backoff badlo** - Server ko `JOB_BACKOFF_BASE=5` se chalao,
   `always_fail` job daalo, aur `GET /jobs/{id}` baar-baar karke
   dekho `run_at` kitna aage badh raha hai (5, 10, 20...).
2. **Priority dekho** - `JOB_WORKER_COUNT=0` karke server chalao
   (koi worker nahi, jobs line me rukenge). Priority 1, 10, 5 wale
   teen jobs daalo. Ab apni chhoti Python script se `JobQueue`
   kholo, `claim_next()` teen baar chalao - order note karo.
3. **Apna handler banao** - `main.py` me ek `resize_image` handler
   likho (nakli: payload se width/height padh ke summary return
   karo), register karo, API se job daal ke succeed hote dekho.
4. **Idempotency tod ke dekho** - Bina key ke ek hi job do baar
   POST karo (do jobs banenge), phir key ke saath do baar (ek hi
   banega). `/stats` me total se verify karo.
5. **DLQ safai** - Ek `always_fail` job ko dead hone do, requeue
   karo, aur dekho wo wapas dead ho jata hai. Socho: asal company
   me requeue se pehle kya "fix" karna padta?
6. **Crash test** - Kuch jobs daalo, server band karo (Ctrl+C),
   dobara chalao. Queued jobs ab bhi hain? `/stats` se prove karo.

## 8. Interview Q&A

**Q1. Job queue kyun use karte hain? Request ke andar hi kaam kar
lene me kya problem hai?**
Slow kaam (email, report, payment call) ko request me karoge to
user wait karega, timeout honge, aur ek service slow hui to puri
app slow. Queue kaam ko background me bhej deti hai - user ko
turant jawab, kaam apni speed se, fail ho to retry ke saath.

**Q2. Exponential backoff kya hai aur kyun zaroori hai?**
Har retry ke beech ka wait dugna karna (1s, 2s, 4s, 8s...). Bina
backoff ke, down service pe saare clients har second try karke use
aur dabaa denge (thundering herd). Backoff dabav kam karta hai aur
chhoti gadbad ko turant recover bhi kar deta hai.

**Q3. DLQ (Dead-Letter Queue) kya hota hai?**
Retries khatm hone ke baad fail jobs ko alag rakhne ki jagah. Delete
nahi karte taaki error dikhe, data na haare, aur bug fix ke baad
unhi jobs ko requeue karke dobara process kar sakein. SQS, RabbitMQ
sab me hota hai.

**Q4. Ek job do workers ek saath utha lein to? Kaise roka?**
Claim ek atomic transaction (`BEGIN IMMEDIATE`) me hota hai - pehla
worker job ko `running` mark karke commit karta hai, dusre ko wo
job `queued` milega hi nahi. Test me 4 threads daud ke prove kiya
hai: 20 jobs, har ek exactly ek baar claim hua.

**Q5. Idempotency kyun chahiye? Job duplicate ban jaye to kya
hoga?**
Network retry ya double-click se ek hi kaam do baar aa sakta hai -
email do baar, payment do baar. Unique `idempotency_key` de ke
enqueue karne se dusri baar purana job hi return hota hai, naya
nahi banta. Payment aur order wale systems me ye sabse zaroori
safety hai.

---

Padh liya? Ab `README.md` ka curl demo khud haath se chalao - jo
cheez haath se chala ke dekhi ho, wo interview me kabhi nahi
bhoolti.

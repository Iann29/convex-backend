# AMAGE Convex backend — Candidate 4

## Candidata escolhida

**A candidata é a variante direct, entregue como `capacity`.** Ela serve as HTTP
actions diretamente na porta 3211, mantém a API cloud na 3210 e conserva **16
vagas por padrão**. O knob `SITE_MAX_CONCURRENT_REQUESTS` permite o perfil de 64
vagas testado com PostgreSQL.

| Variante avaliada                                                                | Decisão                                                           |
| -------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| Pooled: proxy com cliente Hyper compartilhado e keep-alive                       | Alternativa testada; não é a candidata publicada.                 |
| Direct/capacity: listener 3211 atende o router existente, sem forwarding interno | Candidata selecionada. O perfil pesado foi validado nesta imagem. |

O experimento `SQLITE_CACHE_KIB` não integra esta candidata. Não demonstrou
benefício na comparação controlada. Nenhum limite de mutações, regra OCC ou
garantia de durabilidade foi relaxado pelo patch.

## Identidade e procedência

O [manifesto da candidata](amage-candidate4.json) registra a versão, base,
commit de runtime, hash do patch, arquitetura e parâmetros exatos de build. A
publicação reutiliza o artefato validado; não recompila nem altera seus labels.

- Base upstream: `849a6fc605b5a910c9834b61a77c5475cc24d189`.
- Commit dos patches anteriores de SQLite/search/storage/build:
  `d9b60407ca2be1a36e633b3e283d3052c932f3b8`.
- Commit final do runtime C4: `4268bb2a41192924ea68ade229f7303620d672a6`.
- SHA-256 do patch de runtime:
  `a96722fa7fe39f1a542c787894650486b4334e83ea6987e998ec7808820fbb22`.
- Versão compilada:
  `amage-20260907-candidate.4-capacity+base.849a6fc605b5.patch.a96722fa7fe3`.
- Plataforma validada: **Linux/amd64**. Não é uma publicação ARM64.

`org.amage.base-revision` e `org.opencontainers.image.revision` continuam
identificando a base upstream; `org.amage.patch-sha256` identifica as
alterações. O commit de runtime no manifesto faz a ligação com o fork. Os labels
não foram reescritos para sugerir um novo build.

Publicada em 08/09/2026 e puxada novamente pelo digest, com identidade igual ao
artefato testado:

```text
ghcr.io/amageweb/convex-backend@sha256:0fb869fc1a614c68e28cd003ebe17e77642d65a70fcc38d2184cfa01b54e57da
```

Tag:
`ghcr.io/amageweb/convex-backend:amage-20260907-candidate.4-capacity-a96722fa7fe3`.
Usar `digestReference` para fixar a imagem; a tag identifica a candidata, mas
não substitui um digest. O pacote GHCR exige autenticação de uma conta
autorizada.

Para conferir o código, a partir da raiz desta branch:

```bash
git diff --binary --no-ext-diff --no-color --src-prefix=a/ --dst-prefix=b/ \
  --abbrev=9 849a6fc605b5a910c9834b61a77c5475cc24d189 \
  -- crates self-hosted/docker-build/Dockerfile.backend | sha256sum
```

Resultado esperado:
`a96722fa7fe39f1a542c787894650486b4334e83ea6987e998ec7808820fbb22`. Documentação
e ferramentas de teste não entram no hash de runtime.

## Mudanças verificadas

- [SQLite](../crates/sqlite/src/lib.rs): preserva paginação de
  documentos/índices, filtro por tabela, revisões e tombstones. O SQL da página
  de índice percorre a chave ordenada e busca a revisão mais recente por chave,
  evitando agrupar/ordenar o intervalo inteiro a cada página. Não muda o schema
  físico.
- [Listener site](../crates/local_backend/src/site.rs): usa o router HTTP
  existente, timeout, shutdown e identidade de métricas `backend_http_proxy`;
  padrão de 16 vagas. Zero em `SITE_MAX_CONCURRENT_REQUESTS` é rejeitado. As
  portas 3210 e 3211 continuam com papéis separados.
- [Composição](../crates/local_backend/src/main.rs) e
  [router](../crates/local_backend/src/router.rs): compartilham a mesma
  aplicação. O antigo cliente/proxy interno foi removido. Flags de bind/site,
  métodos, streaming e metarrotas foram preservados nos testes.
- [Storage local](../crates/storage/src/lib.rs): atributos usam metadados do
  arquivo, sem ler seu conteúdo inteiro; buffer local limitado a 5 MiB,
  preservando arquivos maiores que 10.000 partes. O multipart S3 não foi
  alterado.
- [Search](../crates/search/src/archive/cache.rs): preserva limpeza de extrações
  incompletas e diretórios readonly sem seguir symlinks, cache configurável e
  leitores de segmentos imutáveis com reload manual.
- [Build](docker-build/Dockerfile.backend): preserva builder Bookworm fixado,
  propagação de versão/base/hash e Node 22.22.2. Cache de search da imagem
  continua em 2.048 MiB.

## Evidência medida e limites

**Meta original, 240 msg/min:** 120 segundos por célula, dois webhooks/mensagem,
três empresas, 3.000 documentos e espera assíncrona de 500 ms por handler.
Backends com quota de 2 CPUs; armazenamento tmpfs. C4 direct: gap p95 de **1,32
ms SQLite / 1,06 ms PostgreSQL**, máximos **2,28 / 1,99 ms**. Todas as 1.920
requisições dessas duas células ficaram abaixo de 20 ms; nenhum evento ausente
ou duplicado. C3 tinha gap p95 de 2,26/2,51 segundos nas mesmas condições.

**SQL isolado:** SQLite 3.46.0, 330 mil chaves, cinco amostras aquecidas,
equivalência de resultados. Páginas de 100 linhas: ASC 57,56→0,92 ms; DESC
97,49→0,94 ms. Isso mede SQL e cópia de resultados, não a latência de um webhook
completo. Os oito testes Rust de paginação/equivalência passaram no patch de SQL
empregado pela C4.

**Carga pesada:** 100 empresas × 50 msg/min, 330 mil documentos iniciais, uma
mutação por webhook. Cada mutação lê o contador da empresa e 100 itens, insere
59 payloads e um receipt e atualiza o contador: aproximadamente **488 linhas
físicas por mensagem**, com OCC ativo. Uma espera assíncrona de 200 ms ocorre
depois da mutação. A contagem paginada confirma todos os IDs e os 100
contadores.

VM Namespace com 16 vCPU/32 GiB, volumes em disco; backend com quota de 6 CPUs/8
GiB, PG com 4 CPUs/4 GiB. Resultados de uma rodada de cinco minutos:

| Métrica                                            | Medido                                  |
| -------------------------------------------------- | --------------------------------------- |
| Webhooks concluídos                                | 50.000, zero erros e contagens corretas |
| HTTP p95                                           | 322,84 ms, incluindo a espera de 200 ms |
| Gap emissor × log p95 / p99 / máximo               | 0,66 / 3,33 / 501,86 ms                 |
| Drenagem após emissão                              | 311,57 ms                               |
| Retentativas OCC                                   | 24.987                                  |
| Checkpoints concluídos na janela                   | 1                                       |
| SIGKILL de backend e PG → recuperação e recontagem | 47,53 s, contagens preservadas          |

Os [resultados estruturados](bench/candidate4-results.json) também incluem os
controles: com quota PG de 2 CPUs, o gap p95 chegou a 1,83 s na janela longa;
com 4 CPUs e LZ4, WAL caiu 6,8%, mas gap p99 e recuperação pioraram nesta
comparação. O perfil selecionado mantém compressão desligada.

**Limites:** dados sintéticos uniformes; não incluem o aplicativo comercial,
workpool/aggregate/IA, ingress externo, TLS ou histórico de 37 GB. Igualar
linhas físicas não iguala UDFs, transações SQL ou custo dos componentes. Uma
rodada por perfil não produz intervalo de confiança. SQLite pesado continuou
acumulando fila; WAL FULL ajudou, sem demonstrar capacidade sustentada de 5.000
msg/min. SIGKILL depois dos acknowledgements não simula perda do host/disco ou
falta de energia.

## Perfil PostgreSQL de laboratório

O perfil validado combina os seguintes valores; não é uma alteração automática
dos defaults da imagem:

| Camada          | Valor                                                                                                                                                            |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Backend         | quota de 6 CPUs; memória 8 GiB                                                                                                                                   |
| Backend env     | `SITE_MAX_CONCURRENT_REQUESTS=64`, `MAX_BYTES_WRITTEN_PER_SECOND=16777216`                                                                                       |
| PostgreSQL 18.6 | quota de 4 CPUs; memória 4 GiB                                                                                                                                   |
| PG parâmetros   | `max_wal_size=8GB`, `shared_buffers=1GB`, `wal_compression=off`                                                                                                  |
| PG preservado   | `synchronous_commit=on`, `fsync=on`, `full_page_writes=on`, `checkpoint_timeout=300s`, `checkpoint_completion_target=0.9`, `work_mem=4MB`, `max_connections=100` |

Quotas são tetos de CPU, não reservas de núcleos. Esse orçamento não equivale a
executar toda a pilha numa máquina compartilhada de 6 vCPU. WAL maior exige
espaço e pode ampliar o replay após queda; `max_wal_size` não é uma cota rígida
de disco. **64 vagas isoladamente pioraram alguns casos; 16 permanece o
padrão.**

As variáveis acima pertencem ao processo/container do backend, não ao ambiente
das funções JavaScript. O modo WAL SQLite, quando ensaiado, foi configurado
offline no arquivo existente com `PRAGMA journal_mode=WAL`, preservando FULL;
não há um novo env para habilitá-lo. Reverter a imagem não reverte esse PRAGMA
persistente.

## Validação reproduzível

O [executor de aceite](test-candidate.mjs) valida identidade e patch contra a
base, SQLite/PG, documentos, bytes de arquivos, Node actions/callbacks,
recuperação após erro, realtime/reconexão, busca, jobs vencidos após restart e
export ZIP com CRC/IDs/hashes. Cria exclusivamente containers/redes/volumes
sintéticos, sem portas publicadas, e os remove ao terminar.

Pré-requisitos: Linux, Docker local acessível via `sudo -n`, Node, Python 3,
`bsdtar`, `nsenter` e uma checkout com CLI/SDK Convex compiladas. O aceite
registrado usou CLI 1.43.0. `CONVEX_CLI_REPO` aponta à raiz dessa checkout,
contendo `npm-packages/convex/dist/cli.bundle.cjs` e
`dist/cjs/browser/index.js`.

```bash
# A partir da raiz do fork; definir o caminho da checkout com CLI compilada.
export CONVEX_CLI_REPO=/path/to/built-convex-checkout
export CANDIDATE_CPUSET=0
AMAGE_IMAGE=$(node -p "require('./self-hosted/amage-candidate4.json').digestReference")
sudo -n docker --host unix:///var/run/docker.sock pull "$AMAGE_IMAGE"
node self-hosted/test-candidate.mjs "$AMAGE_IMAGE"
```

Na preparação para publicação, o aceite completo passou novamente a partir desta
worktree commitada, nos dois motores. A imagem anterior C3 foi rejeitada pelo
guard de identidade antes de criar recursos. O artefato publicado foi puxado
pelo digest e passou probes de ABI, versão do backend e Node.

O smoke do benchmark realocado passou com 16 vagas no CPU lógico 0, incluindo
contagens, SIGKILL, recuperação e análise dos logs. A tentativa com 64 vagas
nesse CPU único atingiu pico de 62 e falhou na asserção de saturação antes da
carga; recursos foram removidos. Ela não foi contada como um benchmark aprovado.
A prova de 64 vagas continua sendo a medição no Namespace com recursos
equivalentes aos descritos acima.

O notebook de desenvolvimento marcou TSC como instável; o aceite local foi
limitado ao CPU lógico 0. A dependência `fastant` também falhou com a C3 nesse
host. No Namespace, 100 partidas de cada C3/direct passaram sem essa restrição.
CPU 0 é uma adaptação do ensaio funcional local, não um perfil de desempenho.

Para repetir o benchmark numa máquina descartável com recursos equivalentes,
definir `CANDIDATE_CPUSET` para os CPUs lógicos disponíveis (no host medido,
`0-15`), usar diretório de saída novo e executar sequencialmente:

```bash
AMAGE_RESULTS=$(mktemp -d /tmp/amage-c4-bench.XXXXXX)
node self-hosted/bench/heavy100.mjs --image "$AMAGE_IMAGE" --engine postgres \
  --backend-cpus 6 --postgres-cpus 4 --site-slots 64 \
  --pg-profile wal8-buffers1g --write-mib 16 --seconds 300 --delay-ms 200 \
  --rate 5000 --seed-docs 330000 --max-sockets 512 --stats \
  --out "$AMAGE_RESULTS/postgres-5k"
python3 self-hosted/bench/analyze-http.py "$AMAGE_RESULTS/postgres-5k"
```

O harness pareia cada `audit_id` com seu log antes dos percentis. O gap inclui a
espera no Agent HTTP; `gapAfterSocketMs` a separa. O log termina nos headers e o
emissor lê o corpo inteiro. Verificar `summary.json`, `requests.json`,
`pg-before-load.json`/`pg-after-load.json`, `crash-recovery.json` e
`cleanup.json`. Gates: 50.000 IDs únicos, 100 contadores corretos, zero erros,
p95 do gap <20 ms, drenagem sem acúmulo crescente e checkpoint concluído dentro
da carga. Guardar também p99/máximo e tempo de recuperação.

## Reconstrução

Os argumentos do build original estão no manifesto. Exemplo equivalente para
Linux/amd64:

```bash
sudo -n docker --host unix:///var/run/docker.sock build --platform linux/amd64 \
  -f self-hosted/docker-build/Dockerfile.backend -t amage-c4:rebuild \
  --build-arg CARGO_BUILD_JOBS=2 \
  --build-arg VERGEN_GIT_SHA=849a6fc605b5a910c9834b61a77c5475cc24d189 \
  --build-arg VERGEN_GIT_COMMIT_TIMESTAMP=2026-09-05T23:58:23Z \
  --build-arg AMAGE_PATCH_SHA256=a96722fa7fe39f1a542c787894650486b4334e83ea6987e998ec7808820fbb22 \
  --build-arg CONVEX_RELEASE_VERSION=amage-20260907-candidate.4-capacity+base.849a6fc605b5.patch.a96722fa7fe3 .
node self-hosted/test-candidate.mjs amage-c4:rebuild
```

APT/runtime ainda têm insumos flutuantes: reconstruir não garante bytes/digest
idênticos. A referência publicada identifica o artefato efetivamente testado.
Publicação da candidata não implica deploy, atualização de catálogo/default,
migração ou validação do aplicativo real.

# Candidata 5 — publicada para validação

Commits sobre a C4 publicada (`54b5cc413a2f469c725231b82d3832082651d4d1`),
em revisão no [PR #2](https://github.com/Iann29/convex-backend/pull/2),
rascunho. A identidade da imagem continua sendo o diff completo do runtime
contra o upstream `849a6fc605b5`, agora incluindo `Cargo.lock`. A candidata
selecionada está publicada para validação; **nenhum deploy** foi feito.
**Medido** identifica execução com artefato; **verificado** identifica código
lido; **hipótese** identifica ganho ainda não demonstrado. As seções de
scheduler, HTTP, runtime e boot são do editor deste arquivo; as de SQLite e
batching, do autor da implementação.

## P4 — scheduled mutation sobrevive ao system timeout

**Verificado (C4):** `DATABASE_UDF_SYSTEM_TIMEOUT` mede o tempo acumulado que um
UDF passa pausado em syscalls (`crates/isolate/src/timeout.rs`). Ao estourar, o
isolate é terminado com um erro `BadRequest` de short message
`SystemTimeoutError` (`crates/isolate/src/termination.rs`), e o scheduler trata
todo `BadRequest` como erro determinístico de usuário: a scheduled mutation é
marcada `failed` na primeira tentativa, sem retry
(`crates/application/src/scheduled_jobs/mod.rs`, ramo
`is_deterministic_user_error`). Nada é commitado: a transação, incluindo o
registro `inProgress` preparado antes de executar, é descartada. Um loop que
se reagenda a si mesmo, como o `loop:main` do workpool, perde a próxima
geração; o componente só reage no healthcheck de 30 minutos.

**Verificado (patch):** o ramo determinístico passa a excluir
`SystemTimeoutError`, que segue o caminho de erro de sistema já existente:
`execute_job` → `schedule_retry`, com `attempts.system_errors += 1`, backoff
exponencial com jitter de 500 ms até `SCHEDULED_JOB_MAX_BACKOFF` (2 h por
padrão), log de erro e contador `scheduled_job_result_total` por tentativa. O
job aguarda fora das vagas do executor. Actions não mudam: uma action cujo
`runMutation` estoura o timeout continua terminando `failed`. Não há knob novo;
a semântica é a mesma aplicada a qualquer outro erro de sistema. Um job que
estoura sempre continua visível como falhas repetidas, em vez de encerrar a
cadeia em silêncio.

**Anotação operacional (verificada):** a mudança só alcança execuções que
falham **depois** da troca de imagem. Um job que já terminou `failed` na C4
não é ressuscitado: `SchedulerModel::complete` remove o `next_ts` e grava
`completed_ts` (`crates/model/src/scheduled_jobs/mod.rs`), o executor só
consulta jobs com `next_ts`, e o registro é coletado após a retenção. Se um
loop já estava `failed` antes da troca, a recuperação é do componente
(healthcheck/`forceKick` do workpool) ou um novo agendamento manual; a troca
de imagem sozinha não o reanima.

**Limite:** o upstream marca o timeout como `BadRequest` porque algumas funções
o atingem de forma determinística. Nesse caso a mudança troca uma falha
terminal por retentativas espaçadas até 2 h, cada uma registrada. Não se promete
recuperação em 60 s: a espera é a do backoff persistido. Retry de mutation não
duplica efeitos: a mutation é transacional e o retry é o mesmo mecanismo já
usado para OCC. Nenhuma promessa de exatamente-uma-vez para actions.

## Gate E4 — recuperação sob timeout forçado

O [gate de recuperação](test-recovery.mjs) força o timeout sem tocar em
código, mas não basta zerar o orçamento. **Verificado:** o poller do timeout
(`crates/isolate/src/timeout.rs`, `go` e `termination_reason_or_wait`) só
acorda no deadline de usuário, 1 s após o início da execução, ou ao fim de uma
pausa que ele já observava; o início de uma pausa não o acorda. Com
`DATABASE_UDF_SYSTEM_TIMEOUT_SECONDS=0`, morre com `SystemTimeoutError` apenas
a função que ainda está pausada em t = 1 s. Uma função rápida termina antes e
sobrevive ao knob. A primeira versão deste gate assumiu que o knob sozinho era
determinístico; ele passou três vezes no controle C4 porque as funções eram
lentas no boot frio de um único CPU, e falhou na C5-a quando a query de sonda
respondeu em 19 ms. A correção é uma injeção real: `FUNRUN_ISOLATE_ACTIVE_THREADS=1`
e uma V8 action de laço longo mantida em execução pelo harness. Ela segura o
único permit de isolate ativo, e toda outra execução fica parada na pausa
`concurrency_permit_reacquire` na primeira syscall, ainda pausada em t = 1 s.
Essa pausa é uma das causas reais listadas nos logs de system timeout do
incidente do workpool, não um artifício. O fixture `PauseClient` do OSS é um
stub (`crates/common/src/pause.rs`) e o crate `application` não tem testes,
portanto a injeção por seam Rust proposta na auditoria não existe neste
repositório; o gate de imagem é a alternativa.

**Verificado durante a construção do gate, com log bruto:** só morre quem
faz uma ida real ao banco. `db.insert` é bufferizado no write set da transação
de forma síncrona e nunca solta o permit; uma leitura (`db.query`,
`queryStreamNext`) é uma syscall assíncrona que solta o permit por mais de
100 µs, tempo suficiente para o hog na cabeça da fila tomá-lo
(`crates/isolate/src/concurrency_limiter.rs`, `ConcurrencyPermit::drop`
notifica um waiter de baixa prioridade). Por isso os jobs-alvo leem três vezes
antes de escrever ou de lançar erro, como fazem os handlers reais. Também
verificado: o `scheduler.cancel` de uma Node action é rejeitado pelo validador
de argumentos do node-executor desta árvore, que exige um campo `version`
(`npm-packages/node-executor/src/syscalls.ts`, `syscallCancelJob`) que o
próprio `scheduler_impl.ts` não envia; o gate cancela pelo endpoint admin
`POST /api/cancel_job` (`crates/local_backend/src/scheduling.rs`), em Rust.

Quatro jobs são agendados em instantes separados por 8 s com knobs normais
(funções que chegam juntas revezam o permit entre si e escapam): **T1**
(mutation transitória), **F** (mutation que lança erro), **N** (Node action que
grava via `runMutation`) e **E** (mutation a cancelar). O backend para antes do
vencimento, volta com o knob em 0 e o permit esgotado por `--persist-seconds`,
e volta de novo com knobs normais. Nessa fase queries também morrem; a
observação usa `/metrics`, logs e uma Node action de cancelamento, que chega ao
backend pelo callback `/api/actions/cancel_job` em Rust, sem permit de isolate. Env de laboratório
declarado na saída: `SCHEDULED_JOB_MAX_BACKOFF_SECS=60`, só para limitar a
espera da última fase. O deploy do código acontece antes do knob 0, porque o
mesmo knob alimenta os ambientes de analyze/schema/auth.

```bash
export CONVEX_CLI_REPO=/path/to/built-convex-checkout
export CANDIDATE_CPUSET=0
node self-hosted/test-recovery.mjs IMAGEM --persist-seconds 300 --expect c4|c5 --out resultado.json
```

**Medido — controle C4** (`amage-audit:candidate4-capacity`, 300 s, CPU 0,
SQLite e PostgreSQL com resultados idênticos):

| Observação                                               | C4                                             |
| -------------------------------------------------------- | ---------------------------------------------- |
| Backend com knob 0 boota e responde `/version`           | 30/30 amostras                                 |
| Query sob knob 0                                         | falha com o timeout de system operations       |
| `scheduled_job_result_total{status="success"}` na fase 0 | +4, com os quatro UDFs falhando                |
| Linhas de retry do scheduler no log                      | 0                                              |
| Cancel via Node action sob knob 0                        | action executa; cancel rejeitado, job já failed |
| Estados após normalizar                                  | T1, F, N, E = `failed`                         |
| Mensagens gravadas por T1 / N / E                        | 0 / 0 / 0                                      |

O contador de sucesso do executor cresce quando o UDF falhou: ele mede
`execute_job` retornar `Ok`, não digestão. Não usar essa série como "jobs
concluídos".

**Medido — like-for-like C4 × C5-a** (mesma injeção, 300 s, CPU 0, SQLite e
PostgreSQL; brutos no registro da C5, `fable-gate-e4-results/`):

| Observação                                  | C4 (sqlite / pg)         | C5-a (sqlite / pg)             |
| ------------------------------------------- | ------------------------ | ------------------------------ |
| Injeção válida (T1 e F com timeout)         | sim / sim                | sim / sim                      |
| Retries do scheduler em T1, F               | 0, 0 / 0, 0              | 1, 14 / 16, 14                 |
| T1 após normalizar                          | `failed`, 0 msg          | `success`, 1 msg (nos dois)    |
| F (erro determinístico)                     | `failed`                 | `failed`                       |
| N (Node action que grava via `runMutation`) | `failed`, 0 msg          | `failed`, 0 msg                |
| E (cancelado na fase 1)                     | `canceled`, 0 msg        | `canceled`, 0 msg              |
| `scheduled_job_result_total` na fase 1      | success +4 / +4          | developer_error +16 / +30      |

No SQLite da C5-a, T1 morreu uma vez e a segunda tentativa passou ainda na
fase persistente, por uma brecha na starvation; no PostgreSQL morreu 16 vezes
e só passou depois de normalizar. Nos dois casos a diferença contra a C4 é a
mesma: retry após `SystemTimeoutError` em vez de `failed` terminal. Actions
continuam sem retry e o cancelamento vence a retentativa.

**Medido — aceitação e variante B:** o [executor de aceite](test-candidate.mjs)
completo passou na C5-a e na C5-b (SQLite e PostgreSQL, identidade pelo
manifesto C5 com `Cargo.lock` no hash). A mesma prova de recuperação, em
SQLite por 60 s, passou na C5-b: T1 com 7 retentativas classificadas por
timeout de execução e depois `success`, F `failed`, N `failed`, E `canceled`.

## P5 — fila de admissão HTTP medida de dentro

**Verificado (C4):** `GlobalConcurrencyLimitLayer` adquire a vaga em
`poll_ready`, que roda para a pilha inteira antes de qualquer `call`. O log de
acesso, as estatísticas por rota e o timeout do serviço só começam depois. A
espera pela vaga era invisível ao processo e só aparecia pareando emissor e log
de fora, como nos ensaios da C4.

**Verificado (patch):** uma layer mais externa cronometra o próprio
`poll_ready` e registra `http_admission_wait_seconds{service}`, com
`service="backend"` na 3210 e `backend_http_proxy` na 3211. O limitador, o
log, as estatísticas e o timeout ficam intactos. Uma amostra por ciclo: o
primeiro `poll_ready` arma o relógio, o `Ready` seguinte registra, polls
repetidos antes de `call` não somam, `call` abre o próximo ciclo; tempo ocioso
entre `Ready` e `call` não conta; clone de um service pendente começa ciclo
próprio; service descartado enquanto espera não registra. Teste unitário
`admission_wait_tests` cobre exatamente essa sequência com um service falso.

**Semântica e exclusões:** o relógio começa no primeiro poll do request, depois
de o servidor ler os headers e agendar a task da conexão. Não inclui backlog
TCP, fila de socket no kernel, TLS nem latência de agendamento do runtime. O
resíduo `gap externo − admission` é transporte e runtime, e cresce sob
saturação de CPU. Não se espera igualdade com o gap do harness; a validação
(G1) compara contagem exata por serviço, média dentro de ±10 % e a mesma ordem
de grandeza dos percentis, em três células: 16 vagas a 240 msg/min, 4 vagas a
240 msg/min emulando a C3, e 16 vagas a 960 msg/min. **Ainda não medido.**

## Métricas do runtime Tokio

**Verificado (C4):** o processo registra apenas o coletor de tasks do
`tokio-metrics-collector` (`crates/common/src/runtime/mod.rs`, ao criar
`GLOBAL_TASK_MANAGER`); o coletor de runtime existe no mesmo crate, compilado
porque o workspace ativa `tokio_unstable`, mas `ProdRuntime::init_tokio` nunca o
registrava. Sem ele não há como ver, por dentro, o custo de `block_in_place`
(handoff de worker) nem a saturação do runtime que explica o resíduo entre o
gap externo e a fila de admissão.

**Verificado (patch):** `register_tokio_runtime_metrics(handle)` registra o
`RuntimeCollector` no mesmo registry, uma chamada logo após construir o
runtime. Séries novas, sem namespace: `tokio_workers_count`,
`tokio_total_park_count`, `tokio_total_noop_count`, `tokio_total_steal_count`,
`tokio_total_steal_operations`, `tokio_num_remote_schedules`,
`tokio_total_local_schedule_count`, `tokio_total_overflow_count`,
`tokio_total_polls_count`, `tokio_total_busy_duration`,
`tokio_global_queue_depth`, `tokio_total_local_queue_depth`,
`tokio_budget_forced_yield_count`, `tokio_io_driver_ready_count` e
`tokio_elapsed`. As `tokio_total_*` são cumulativas (usar deltas entre dumps);
as `*_depth` são instantâneas. Um scrape vale pelas duas portas, que
compartilham o registry. Registrar duas vezes é um no-op; outros erros do
registry falham o boot.

## Achado de instrumentação: `table_summary_finish_bootstrap` sempre `error`

**Verificado (C4 e C5-a, código herdado):**
`convex_local_backend_database_table_summary_finish_bootstrap_seconds{status="error"}`
cresce em boots bem-sucedidos. O timer é criado em
`crates/database/src/committer.rs:721` como `_timer` e nunca recebe
`.finish()`; o `StatusTimer` rotula `error` no drop
(`crates/metrics/src/timer.rs`). O `Ok(())` é enviado em `:758` logo antes do
drop. Portanto o rótulo `error` dessa série não indica falha de boot; o valor
do tempo continua válido. Ler apenas a duração e ignorar o status até um
patch futuro chamar `finish()`. A fonte congelada da C5 não foi alterada por
isso.

## SQLite — admissão assíncrona (P6) e prepared por lote (P12)

Texto do autor da implementação (Codex), integrado sem alteração de conteúdo.

**Verificado:** P6 mantém uma conexão SQLite e o Mutex. Um Semaphore de uma vaga espera assincronamente antes de entrar no helper existente block_in_place; o bloco cobre uma página ou todo o lote de revisões. Uma transação aberta dentro do bloco termina por commit/rollback antes de retornar; cancelar enquanto espera o permit executa zero SQL. Não há leitor paralelo nem spawn_blocking destacado. Fonte na candidata P12: crates/sqlite/src/lib.rs:121 (admissão/handoff), :155 (lock/hold), :735/:775 (prepare uma vez por lote). Os ramos current_thread/sem runtime preservam execução inline e são identificados por flavor.

**Medido localmente:** P6 passou 11 testes, incluindo heartbeat com controle negativo inline num runtime de 1 worker, cancelamento com contador 0 SQL e rollback/reabertura. P12 passou 12, acrescentando rebind hit/miss/tombstone/lote vazio nas duas APIs. Patch P12=2493225a3ac91b4fe457f328e2ebb123fa7eddfc3f91668cf60f522daeae15fa; revisão independente Fable aprovada.

**G3 medido no Namespace:** 330 mil documentos / 100 empresas uniformes, 120 s × 5.000 msg/min = 20 mil webhooks, SQLite WAL/FULL, 6 CPUs / 8 GiB, 64 vagas, 16 MiB/s, 512 sockets. A instrumentada sem P6; B com P6, ordem A–B–B–A, mesma VM, fixture nova. Drenagem 82,046 / 77,127 / 50,961 / 49,605 s. Gap após obter socket p95 5.972,648 / 5.321,665 / 4.160,355 / 4.338,162 ms e p99 6.399,998 / 7.842,989 / 4.207,041 / 4.509,295 ms. Todas corretas: 20 mil IDs / 100 contadores, zero erros/OOM,quick_check e recontagem após SIGKILL.

**Interpretação:** medianas B/A drenagem −2,7 %, gapAfterSocket p95 −8 %, p99 +10,5 %; deriva do controle maior que sinal, sem vencedor de throughput. Benefício específico consistente: resíduo médio gapAfterSocket−admissão cai de 60,51–71,95 ms para 0,44–0,59 ms; inclui transporte/agendamento, não é um cronômetro exclusivo de runtime. Não significa webhook 120× mais rápido. Lock wait p95 em B ~1µs e flavor 100 % multi; a fila mudou para o permit, não sumiu. A conexão ficou ocupada 96–98 % do tempo. Custo: A1/B1 parks 9.576 / 1.273.929; busy do runtime exclui o trabalho transferido e não é CPU. Brutos: namespace/g3-local/measurements, resource-summary.json e g3-summary.json no relatório C5.

**P12 medido separadamente:** B–B+P12–B+P12–B. Custo médio previous_revisions 0,7252 / 0,4205 / 0,4703 / 0,8722 ms, ~44 % menor nas medianas com P12; mesmo SQL/retention/rebind. Drenagem 49,945 / 52,062 / 70,371 / 105,408 s: controle mais que dobrou, então a queda descritiva de 21 % da mediana não prova ganho end-to-end. A seleção de B+P12 como candidata publicada se justifica pelo custo específico consistente e mudança mínima com teste/revisão, não pelo throughput. Brutos: namespace/p12-local/measurements.

**Limites:** SIGKILL foi após todos os acks, não durante commit; nenhum teste no arquivo de 37 GB, nenhuma prova de falha de energia, nenhum pool de leitores implementado. A distribuição uniforme sintética não representa o tenant dominante, aggregates ou ledger de IA do Uze. WAL foi configuração de laboratório; a imagem preserva os defaults. G4 e o tuning de batching estão fechados abaixo.

## Imagens desta rodada e publicação

Carregadas de `tar.zst` com SHA-256 conferido e identidade verificada pelo
executor de aceite com o manifesto C5. A candidata **C5-b-p12** foi publicada
em 08/09/2026 por retag da imagem já testada, sem rebuild:

```text
ghcr.io/amageweb/convex-backend@sha256:fd744b3dbe917df467be827157192171e7a869da4bb87be346a35bf559df92eb
```

Tag: `ghcr.io/amageweb/convex-backend:amage-20260908-candidate.5-b-p12-795b36a9298e`.
Manifesto amd64 `sha256:783b7d209c85…`, config `sha256:e5427d62ab20…`; o índice
traz também o descritor de atestação do buildx. **Verificado de forma
independente:** o pull pelo digest devolve o mesmo Image ID da imagem testada
(`fd744b3d…`) e as mesmas labels de versão, base e patch. Publicar não é
deployar: nenhum deployment, catálogo ou default foi alterado.

| Imagem                          | Conteúdo                       | Patch (SHA-256, 12)  | Aceite completo | Gate E4              |
| ------------------------------- | ------------------------------ | -------------------- | --------------- | -------------------- |
| `amage-audit:candidate5-a`      | C4 + P4 + P5 + runtime + instr | `34b7a0dacd51`       | SQLite e PG     | 2 motores, 300 s     |
| `amage-audit:candidate5-b`      | C5-a + P6                      | `48b1cdb797bd`       | SQLite e PG     | SQLite, 60 s         |
| `amage-audit:candidate5-b-p12`  | C5-b + P12                     | `795b36a9298e`       | SQLite e PG     | não repetido (P4 idêntico) |

## Boot com busca (G4, observacional)

Texto do autor da medição (Codex), integrado com tipografia normalizada e sem
alteração de conteúdo. Não é A/B entre imagens: mede o boot da C5-b-P12 nos
dois motores, 330 mil documentos com `searchIndex` `search_body` populado,
300 s a 5.000 msg/min (50 mil webhooks), 100 empresas. Contadores exatos,
zero erros de carga/OOM, reabertura e SIGKILL após os acks, `quick_check` ok
no SQLite. Brutos em `namespace/g4-local` (`boot-summary.json` por motor e
`g4-summary.json`).

**Como ler os tempos:** todos contados desde o `docker start`. "Readiness" é
o `/version` responder; a "janela estável" começa quando a sonda de leitura,
iniciada depois da prontidão, passa a responder sem erro, e é confirmada 60 s
depois. `/version` **não** promete busca pronta: as consultas iniciais que
falharam com `SearchIndexesUnavailable` foram contadas, não escondidas.

| Restart | PG: readiness | PG: janela estável | PG: bootstrap de busca | SQLite: readiness | SQLite: janela estável | SQLite: bootstrap de busca |
| ------- | ------------: | -----------------: | ---------------------: | ----------------: | ---------------------: | -------------------------: |
| 1       | 435,98 ms     | 484,05 ms          | 554,58 ms              | 467,05 ms         | 5.520,66 ms            | 5.585,73 ms                |
| 2       | 420,63 ms     | 468,55 ms          | 596,17 ms              | 436,12 ms         | 4.990,62 ms            | 5.229,44 ms                |
| 3       | 412,56 ms     | 454,76 ms          | 544,98 ms              | 445,00 ms         | 5.002,45 ms            | 5.196,93 ms                |

Readiness e janela estável são instantes desde o `docker start`; bootstrap de
busca é duração. No PG a janela estável abre cerca de 50 ms depois da
prontidão; no SQLite, cerca de 5 s depois, coincidindo com o fim do bootstrap
de busca.

Revisões replayadas no bootstrap de busca: PG 1.577 carregadas e 96.199
descartadas (`skipped`) em todos os restarts, 187.519 bytes relevantes, uma
consulta inicial `SearchIndexesUnavailable` por restart; SQLite 10.620
carregadas e 647.822 descartadas, 1.262.721 bytes relevantes, 11/10/10
consultas iniciais `SearchIndexesUnavailable` e depois estável. PG e SQLite
partiram de checkpoints diferentes: **não** atribuir a razão entre os tempos
apenas ao motor. Sem replay menor nos restarts 2 e 3: nenhum checkpoint
avançou e as mesmas revisões voltaram. Summary bootstrap 9,4–23,6 ms; summary
finish 3,5–10,3 ms com `status=error` apesar do sucesso (ver achado acima);
shapes 7,4–20,6 ms.

**Conclusão:** não justifica reduzir a idade global de checkpoint para 600 s
(P11 sem patch nem knob nesta C5). O que aparece nos dois motores é o stream
do bootstrap descartando cerca de 61× mais revisões do que usa
(`stream_revision_pairs_for_indexes` carrega tudo e filtra depois); o
próximo experimento concreto é filtrar por tabela/índice no stream. Essa
razão pertence a esta fixture; nada foi medido no arquivo de 37 GB nem em
produção.

## Batching de escrita no SQLite (tuning de laboratório, sem mudança na imagem)

Medido pelo autor do SQLite (Codex) numa segunda VM, ordem espelhada
(default → 256 docs → 256 + um escritor → 256 + um escritor → 256 docs →
default), mesma imagem publicada, 6 CPUs / 8 GiB, WAL/FULL, 16 MiB/s, 64
vagas, 330 mil documentos / 100 empresas, 120 s a 5.000 msg/min. Knobs do
**processo** do backend: `COMMITTER_MAX_WRITE_BATCH_DOCUMENTS=256`,
`COMMITTER_MAX_WRITE_BATCH_BYTES=262144` e, no "um escritor",
`COMMITTER_MAX_CONCURRENT_WRITE_BATCHES=1`; `delay` de 1 ms inalterado.
Sempre 20 mil commits lógicos / 1,22 milhão de documentos por célula.

| # | Perfil            | Drenagem | gap após socket p95 / p99 | Commits por lote | p95 de `database_commit_seconds` | Lotes físicos |
| - | ----------------- | -------: | ------------------------: | ---------------: | -------------------------------: | ------------: |
| 1 | default           | 64,6 s   | 4.910 / 5.071 ms          | 1,46             | 77–88 ms                         | 13.690        |
| 2 | 256 docs          | 28,1 s   | 4.036 / 4.196 ms          | 1,76             | 68–77 ms                         | 11.354        |
| 3 | 256 + um escritor | 17,1 s   | 3.702 / 3.822 ms          | 3,32             | 215–245 ms                       | 6.031         |
| 4 | 256 + um escritor | 13,8 s   | 3.512 / 3.755 ms          | 4,42             | 215–245 ms                       | 4.521         |
| 5 | 256 docs          | 1,5 s    | 833 / 1.006 ms            | 1,51             | 53–60 ms                         | 13.291        |
| 6 | default           | 27,0 s   | 3.621 / 3.738 ms          | 1,63             | 68–77 ms                         | 12.281        |

As seis células preservaram 20 mil IDs aceitos/únicos/concluídos/reabertos,
100 empresas × 200, zero erros de requisição/OOM, SIGKILL após os acks e
`quick_check` ok. Não é zero OCC: o delta de `occ_retries_total_sum` foi
10.000 / 10.000 / 10.000 / 10.000 / 9.999 / 10.000 por célula, idêntico a
`errors_reported_total{type="occ"}`; são tentativas recuperadas, sem perda
final. CPU média por célula 361–436 %.

**Leitura:** a volta derrubou a ordenação da ida: o default caiu de 64,6 s
para 27,0 s sem mudar nada, e a célula 5 drenou em 1,5 s. A deriva domina.
O mecanismo do escritor único está confirmado (3,3–4,4 commits por lote,
metade dos lotes físicos), **mas** custa cauda de commit cerca de 3× maior
(215–245 ms contra 68–88 ms) e não foi o mais rápido na volta; a fila antes
do committer cai para 1,7–2,2 ms, e a latência total do commit sobe, portanto
não ler a fila isolada. `256 docs` venceu o default nas duas metades, sem
mecanismo consistente na volta. **Decisão:** nenhum override nem default
entra na imagem publicada; `256 docs` é o próximo candidato de tuning para
repetição e para o aplicativo real; "um escritor" fica experimental, não é
recomendação geral. Nenhuma porcentagem causal de throughput a divulgar.

**Limitação operacional (verificada):** são variáveis do processo do backend,
e o Synapse hoje não as persiste por deployment: o loader de ambiente do
create/recreate produz apenas CORS e variáveis de sistema
(`synapse/internal/provisioner/claim.go:306-326`), e `synapse env set` alimenta
o runtime JavaScript. Uma variável inserida à mão no container se perde no
recreate. Este tuning é de laboratório e manual; nenhuma mudança na frota.
Brutos em `namespace2/batch-local/measurements`, arquivo com SHA-256
`3652414fa82f4dd08a0c0056397030b9851c66dd30203cddbc994f09b063c036`.

## O que ainda não existe

Deploy em qualquer deployment, validação com o aplicativo real, o filtro por
tabela/índice no bootstrap de busca, e a repetição do tuning `256 docs` com
o app real. Um mecanismo de persistir knobs do processo por deployment no
Synapse (P15 da auditoria) é pré-requisito para qualquer recomendação de
tuning virar operação.

## C6 — EXPLAIN R10 publicado

Esta branch acrescenta captura autenticada de execuções, contadores, origens
de consultas, tentativas e relações causais. O contrato nativo, os limites e
as regras de interpretação estão em [EXPLAIN.md](EXPLAIN.md). A coleta observa
tráfego existente e preserva as políticas de execução da C5.

A imagem R10 foi publicada em 11/09/2026 e entrou no catálogo do Synapse
v1.91.0 como `c6`, ao lado de `c5` e `official`. A CLI correspondente é a
1.55.0. A publicação não troca a imagem de deployments existentes. O
[manifesto C6](amage-candidate6.json) registra a imagem e o commit exato do
runtime usado no build:

```text
ghcr.io/amageweb/convex-backend@sha256:059efb6eb93c3b3a004ce17a5949516059990551d739c9e0e5cc37b2b33ddbb9
```

O commit `af613dc7af05010a9c7f7d892185383bac9667a9` preserva byte a byte a
árvore exportada para o build, `0e51ca42303bc36ef84a11553ce406d9573275dd`.
A formatação posterior fica fora dessa identidade. Para reproduzir o hash
do patch registrado na imagem, use os mesmos prefixos do patch original:

```bash
git diff --binary --no-ext-diff --no-color --src-prefix=c/ --dst-prefix=i/ \
  --abbrev=9 849a6fc605b5a910c9834b61a77c5475cc24d189 \
  af613dc7af05010a9c7f7d892185383bac9667a9 \
  -- crates Cargo.toml Cargo.lock self-hosted/docker-build rust-toolchain .cargo \
  | sha256sum
```

Resultado: `6a1de1f651efa34189d9bc9511a3741b9519f141d88cae3aca17a5e7143a479b`.

**Desempenho ainda inconclusivo:** a confirmatória R9, com o runtime R7,
registrou 576.000 requisições e 115.200 ACKs de mutations persistidos, sem
falhas de request, lacunas ou duplicatas no ensaio. O p95 estimado foi
−5,081% para C6 OFF contra C5 e +7,304% para C6 ON contra OFF; os critérios
de precisão/estabilidade e o teto de 10% com captura não foram satisfeitos.
A oferta fixa de 100 requests/s não mede capacidade máxima.

R10 troca a cópia profunda dos eventos por `Arc<CapturedEvent>` durante o
snapshot. Os 13 testes `common::explain` e o smoke SQLite de start/poll/stop
passaram nessa revisão. Isso não substitui a repetição da matriz de overhead
com a imagem R10; os resultados R9 não aprovam o desempenho dessa imagem.

### Evidência histórica R7

A revisão experimental R7 foi validada nativamente em 09/09/2026 com a imagem
privada `nscr.io/bbpa030o8d7tg/amage-c6-lab@sha256:598be37050a977a8e5fdf3540673f3186ac78b75e84925f415a228c7bdbccbac`
e o patch de runtime
`79f404be328eb99e210bef8ae9720b38bbd1bb7748e36a41ea787c574fa768c5`:
os dois fixtures completos SQLite/PostgreSQL, 38 gates funcionais, 6 fases de
invalidação de origem e 18 representações HTTP passaram. GET/DELETE gzip
preservaram o JSON decodificado e os cursores das sessões paradas; `q=0`,
HEAD, CORS, autorização, limites, retries e falhas de persistência mantiveram
seus contratos. A prova de expiração de 303 s é herdada, com o caminho
inalterado; não foi repetida nesta revisão.

O pacote privado de entrega contém `R7-FUNCTIONAL-RESULTS.md` e
`lab/r7-functional-evidence.tar.gz` (SHA256
`3c8ed2332455d994ed57413da6b729130571a260d160dc43c6c77325113a5eae`),
com comandos e resultados. Esses nomes referem-se ao pacote da investigação,
não a arquivos publicados neste repositório. A prova nativa, isoladamente,
não declara integração Synapse ponta a ponta, release ou ganho de desempenho.
A redução do corpo HTTP não substitui uma comparação de overhead controlada;
a confirmatória posterior R9 permaneceu inconclusiva, conforme registrado acima.

# Candidata 7 — verified deploy (`amage.verified-deploy/1`)

Imagem **local e não publicada** (`published: false` no manifesto; nenhum
`docker push`, nenhum digest de registry). O que existe é o artefato
construído em 20/09/2026 nesta máquina, identificado pelo
[manifesto C7](amage-candidate7.json), e a evidência de uso real registrada
no laboratório Bendvex (`evidence/c7/e2e.json`, `e2e-postgres.json`).

## Identidade (de `amage-candidate7.json`)

| Campo | Valor |
|---|---|
| base upstream | `849a6fc605b5a910c9834b61a77c5475cc24d189` |
| referência C6 | `41f472c767321302f71ea4419ff71e4bd5ec686d` (`c6Reference`) |
| commit do gate (`patchCommit`) | `874af9e9ce5e9e800d2531cdefeeb53a312d43c3` |
| commit da CLI (`cliCommit`) | `3c59c5f07459e0c96310f4673d62dfaaa6e7799a` — fora de `patchPaths`, não entra na imagem |
| commit do manifesto/labels/script | `ad209700a83860a31afb03dadf9b6217d80fd158` |
| `variant` / `protocol` / `capabilityRoute` | `7-verified-deploy` / `amage.verified-deploy/1` / `/api/amage/verified_deploy` |
| `version` | `amage-20260920-candidate.7-verified-deploy+base.849a6fc605b5.patch.cfbb0ffd9312` |
| `image` (tag local) | `amage-convex-backend:amage-20260920-candidate.7-verified-deploy-cfbb0ffd9312` |
| `imageId` | `sha256:d3ffc0cc450b93c54cb7763ba377ef3557fde06838d4df6ca68ead3f31dedeab` |
| `amd64ManifestDigest` | `sha256:677cd99fa5b58c785e86f10a9a76f71de4ac59d3a2903a7b5a60d2fe50a2eadf` |
| `configDigest` | `sha256:a3d86f11039cc923bd82ce11e449c6d08b930d4d4c03677e05b39b23d04f438d` |
| `binarySha256` (`/convex/convex-local-backend`) | `cd914b7f9fc6ae0d76c613424375406098090dd90228f621acff8b955326b778` |
| `patchSha256` (`a/ b/`) | `cfbb0ffd93124319c902e7abae3283e488b19f3d684c6c92bd3dd0785157c305` |
| `patchSha256MnemonicPrefixes` (`c/ i/`) | `13643fb6607a2f91f43a5236bfe1ae1f48f627decb600b746defc950a7ef30ad` |
| `platform` / `builtAt` | `linux/amd64` / `2026-09-20T17:40:31.901857364-03:00` |

Labels observados na imagem: `org.amage.base-revision`, `org.amage.patch-sha256`,
`org.amage.patch-commit`, `org.amage.verified-deploy.protocol`,
`org.opencontainers.image.revision`, `org.opencontainers.image.version`,
`org.opencontainers.repository`. Os dois novos (`patch-commit`,
`verified-deploy.protocol`) foram acrescentados em
[`docker-build/Dockerfile.backend`](docker-build/Dockerfile.backend) (linhas
124-125 e 131-132) ao lado dos labels AMAGE existentes; nenhum label anterior
foi reescrito.

## O que mudou em `crates` (C6 `41f472c76` → `ad209700a`)

`git diff --stat 41f472c76 ad209700a -- crates`: 10 arquivos, +1042/−8.

| Arquivo | Mudança |
|---|---|
| `crates/local_backend/src/bendvex.rs` | novo módulo: gate `amage.verified-deploy/1` — leitura da chave no arranque, capability, JSON canônico, verificação do envelope (assinatura, digest do pedido, instância, janela, forma do pedido), pendente start→finish |
| `crates/keybroker/src/verified_deploy.rs` (+ `pub mod` em `lib.rs`) | `verify_ed25519` (aws-lc-rs, já dependência) e `parse_rfc3339` (chrono); signer só em `cfg(test)`/`feature = "testing"` |
| `crates/local_backend/src/deploy_config2.rs` | `start_push` preserva o JSON bruto, chama o gate depois da admin key e antes de `Application.start_push`, grava o hash da resposta; `evaluate_push`/`evaluate_schema` recusados sob proteção; `finish_push_internal` consome o pendente |
| `crates/local_backend/src/deploy_config.rs`, `schema.rs` | `push_config` e `prepare_schema` recusados sob proteção |
| `crates/local_backend/src/router.rs` | rota `GET /api/amage/verified_deploy` (sem autenticação) |
| `crates/local_backend/src/lib.rs`, `main.rs` | inicialização do gate em `make_app` e em `run_server_inner` |
| `crates/model/src/source_packages/upload_download.rs` | SHA-256 dos bytes ZIP baixados conferido antes de expor módulos (incondicional; vem do patch experimental `49edc9a1d`) |

Sem alteração em `Cargo.toml`/`Cargo.lock`. Testes: `bendvex::tests` (canonicalização com vetor cruzado com Python, capability, receipts válidas/forjadas/adulteradas, janela, recusas de forma, pendente), `keybroker::verified_deploy::tests` (RFC 8032, round trip, RFC 3339), `upload_download::tests::valid_zip_requires_matching_source_package_hash`.

## Variáveis de ambiente e capability

| Variável | Efeito |
|---|---|
| `AMAGE_VERIFIED_DEPLOY_PUBKEY` | 64 hex minúsculos (32 bytes Ed25519). Presente ⇒ modo **protegido**: `start_push` exige `amageVerified` assinado por essa chave; sem envelope `400 VerifiedDeployRequired`, inválido `400 VerifiedDeployRejected`; `push_config`, `prepare_schema`, `evaluate_push`, `evaluate_schema` recusados. Ausente ⇒ comportamento upstream. Valor inválido aborta o arranque; imutável no processo |
| `AMAGE_VERIFIED_DEPLOY_SUPERVISOR_URL` | opcional; só anunciada na capability |

`GET /api/amage/verified_deploy` responde
`{"protocol":"amage.verified-deploy/1","supported":true,"required":<bool>,"target":{"instanceName":"…"}}`
mais `verifierKeyId` (SHA-256 da chave pública) e `supervisorUrl` quando
protegido. Um backend anterior à C7 responde 404 e a CLI o trata como não
protegido. Nenhuma chave, credencial ou policy é embutida na imagem.

O contrato completo (envelope, JSON canônico, mensagens de erro, API do
supervisor, CLI) está no laboratório: `docs/c7-protocol.md`,
`docs/c7-trust-boundaries.md`, `docs/c7-versioning.md`, `docs/c7-failure-ux.md`.

## Como construir

```bash
self-hosted/build-candidate7.sh                        # árvore limpa e commitada
CANDIDATE_DATE=20260920 self-hosted/build-candidate7.sh    # repetir a string de versão
ALLOW_DIRTY=1 self-hosted/build-candidate7.sh          # árvore suja; o manifesto registra
SKIP_BUILD=1 self-hosted/build-candidate7.sh           # só re-registrar uma imagem já construída
```

O script lê `base`, `patchCommit`, `cliCommit`, `patchPaths`, `variant` e
`protocol` do manifesto, exige que `patchCommit` e `cliCommit` sejam ancestrais
de `HEAD`, calcula o hash do patch com o mesmo comando de
`self-hosted/test-candidate.mjs`, roda `sudo -n docker build --platform
linux/amd64 -f self-hosted/docker-build/Dockerfile.backend` com os build-args
`VERGEN_GIT_SHA`, `VERGEN_GIT_COMMIT_TIMESTAMP`, `AMAGE_PATCH_SHA256`,
`AMAGE_PATCH_COMMIT`, `AMAGE_PROTOCOL`, `CONVEX_RELEASE_VERSION`, etiqueta a
imagem localmente (nunca faz push), confere labels e `--version` do binário e
grava a identidade observada de volta no manifesto. Uma reconstrução **não** é
byte-reproduzível (entradas APT/runtime flutuam): `imageId`/`configDigest`
identificam o artefato que foi testado, não "qualquer build deste commit".

## Como conferir o hash do patch

```bash
git diff --binary --no-ext-diff --no-color --src-prefix=a/ --dst-prefix=b/ \
  --abbrev=9 849a6fc605b5a910c9834b61a77c5475cc24d189 \
  ad209700a83860a31afb03dadf9b6217d80fd158 \
  -- crates Cargo.toml Cargo.lock self-hosted/docker-build rust-toolchain .cargo \
  | sha256sum
```

Resultado: `cfbb0ffd93124319c902e7abae3283e488b19f3d684c6c92bd3dd0785157c305`
(reproduzido em 20/09/2026; é o valor do label `org.amage.patch-sha256` e da
string de versão). Com `--src-prefix=c/ --dst-prefix=i/` — o comando histórico
desta página para a C6 — o resultado é
`13643fb6607a2f91f43a5236bfe1ae1f48f627decb600b746defc950a7ef30ad`
(`patchSha256MnemonicPrefixes`). `git diff … 874af9e9c` sozinho **não**
reproduz o hash gravado, porque a mudança de labels do Dockerfile (commitada em
`ad209700a`) faz parte do diff hasheado.

## Estado

Imagem local, sem publicação, sem entrada no catálogo do Synapse e sem
avaliação de desempenho. O uso real registrado é o E2E do laboratório
(`tools/c7_e2e.py`: container desta imagem em modo protegido, supervisor real,
`npx convex dev/deploy` com a CLI AMAGE 1.45.0; CLI upstream 1.44.0 recusada
com `400 VerifiedDeployRequired`). Publicar, incluir no catálogo e medir são
passos posteriores e não estão implícitos neste registro.

# Candidata 7 — hardening (`amage.verified-deploy/2`)

Segunda imagem da **mesma** C7 (a versão não mudou de nome: mesma linhagem,
mesmo `variant`, novo patch), construída em 20/09/2026 de árvore limpa
(`buildTreeDirty: []`, `headCommit d46388dc2`). Local, não publicada. O que
mudou é o protocolo do gate: a receipt de instalação assinada pelo supervisor
passa a ser a **credencial** de deploy — enviada no campo `adminKey` de
`start_push`/`wait_for_schema`/`finish_push` como token
`amage-receipt/2.<base64url>` — e o backend a transforma numa identidade admin
restrita à operação upstream `Deploy` (`AdminIdentity.allowed_ops`), de modo
que a admin key sai do fluxo `npx convex dev`/`deploy` e o coding principal
não recebe nenhum poder administrativo sobre dados, env, logs, backups ou
funções internas (recusa `OperationNotPermitted` do próprio upstream).
Contrato normativo no laboratório: `docs/c7-hardening-contract.md`.

## Identidade (de `amage-candidate7.json`, após o build)

| Campo | Valor |
|---|---|
| base upstream / referência C6 | `849a6fc605b5a910c9834b61a77c5475cc24d189` / `41f472c767321302f71ea4419ff71e4bd5ec686d` |
| commit do gate v2 (`patchCommit`) | `a425376130dcc30b99184862d4222103a28504b3` |
| commit da CLI (`cliCommit`) | `9676ecb49213e70f37c0719dcec7f4372ef079d2` — fora de `patchPaths` |
| commit do manifesto que apontou para o gate v2 | `d46388dc20978fb89935a11727a1d716cabacb70` (árvore construída) |
| `protocol` | `amage.verified-deploy/2` |
| `version` | `amage-20260920-candidate.7-verified-deploy+base.849a6fc605b5.patch.8d37a6040498` |
| `image` (tag local) | `amage-convex-backend:amage-20260920-candidate.7-verified-deploy-8d37a6040498` |
| `imageId` | `sha256:d168ad9f382a14fe8c7593ef4e3768f4b51ada5f241a8ae1c48e5508a61b9dcf` |
| `amd64ManifestDigest` | `sha256:8352252100862e7cdd8fcc8430734a8c7403d0507e12c5bf8910dd8954e240b1` |
| `configDigest` | `sha256:d42142005ec62c45bede1f1fa92b715ec59450af11acb7da90dc3e5c923f7fe0` |
| `binarySha256` (`/convex/convex-local-backend`) | `990c2132c693924fb8fcb0222c6244c3cabcf49d56d48bd05d68b69ed0f3a398` |
| `patchSha256` (`a/ b/`) | `8d37a6040498768ae3b8a935acda2669ab07828645176ff3dd074defd9e6669b` |
| `platform` / `builtAt` | `linux/amd64` / `2026-09-20T23:25:41.472781716-03:00` |

Labels: os mesmos sete da primeira imagem C7, com
`org.amage.patch-commit=a42537613…` e
`org.amage.verified-deploy.protocol=amage.verified-deploy/2`.

## O que mudou em `crates` (`80f0ae560` → `a42537613`)

Quatro arquivos, todos em `crates/local_backend/src/`; sem alteração em
`Cargo.toml`/`Cargo.lock`; `keybroker` intocado. Fora de `crates`, o patch
hasheado inclui `self-hosted/docker-build/run_backend.sh`: `--port`/`--site-proxy-port`
aceitam `PORT`/`SITE_PROXY_PORT` (necessário com `--network host`, o modo em que
um backend containerizado alcança um supervisor no loopback do host).

| Arquivo | Mudança |
|---|---|
| `bendvex.rs` (reescrito, 3004 linhas, ~1420 de testes) | trust set `AMAGE_VERIFIED_DEPLOY_PUBKEY=<hex64>[@notAfter][,…]` (kid = SHA-256 da chave, retirada por data); tokens `install`/`observe`/`attestation` com conjuntos de campos fechados e verificação na ordem do contrato; `AmageReceiptAuth` (`AccessTokenAuth`) que sintetiza `AdminIdentity` com `allowed_ops=[Deploy]`; tabela de instalações por `installationId` (`starting/started/finishing/finished`, retry idempotente do start, finish no máximo uma vez por processo, 24 h de TTL, 1000 entradas); consulta online `POST <supervisor>/v1/installation_check` no `start` e no `finish` com atestação assinada e nonce (timeout 5 s, fail closed); linha de proveniência gravada como `message` do evento `push_config_with_components`; `GET /api/amage/installations[/{id}]` lendo memória e log de auditoria durável |
| `deploy_config2.rs` | o gate corre **antes** de `must_be_admin_from_key` em `start_push`/`wait_for_schema`/`finish_push_internal`; a `message` do finish é substituída pela linha de proveniência em alvo protegido; `finished()` após o commit upstream |
| `lib.rs` | `ApplicationAuth::new(key_broker, bendvex::access_token_auth(name), runtime)` — `AmageReceiptAuth` quando protegido, `NullAccessTokenAuth` caso contrário |
| `router.rs` | rotas `GET /api/amage/installations` e `GET /api/amage/installations/{installation_id}` |

Testes: `cargo test -p local_backend bendvex` 20/20 (vetor de ouro compartilhado
com `tests/receipts.py` do laboratório, máquina de estados de instalação,
rotação/retirada de chaves, checagem online sobre HTTP real, escopo da
identidade da receipt), `cargo test -p keybroker verified_deploy` 3/3; clippy e
rustfmt limpos.

## Variáveis de ambiente e capability (v2)

| Variável | Efeito |
|---|---|
| `AMAGE_VERIFIED_DEPLOY_PUBKEY` | lista `<hex64>[@YYYY-MM-DDTHH:MM:SSZ]` separada por vírgulas (um único hex = formato v1, ainda válido). Presente ⇒ modo protegido. Entrada com `@notAfter` é chave em retirada, aceita só enquanto `now < notAfter`. Duplicata, hex inválido, timestamp inválido ou mais de 8 chaves abortam o arranque |
| `AMAGE_VERIFIED_DEPLOY_SUPERVISOR_URL` | **obrigatória** no modo protegido (o arranque falha sem ela: `AMAGE_VERIFIED_DEPLOY_SUPERVISOR_URL is required when AMAGE_VERIFIED_DEPLOY_PUBKEY is set`); origem http(s) consultada em `POST <url>/v1/installation_check` |

`GET /api/amage/verified_deploy` →
`{"protocol":"amage.verified-deploy/2","supported":true,"required":true,"target":{"instanceName":"…"},"verifierKeys":[{"kid":"…"},{"kid":"…","notAfter":"…"}],"supervisorUrl":"…"}`
(`verifierKeyId` deixou de existir). Sem proteção: `required:false` e as rotas de
instalação respondem 404.

Em alvo protegido: admin key cifrada (ou nada) em `start_push` ⇒
`400 VerifiedDeployRequired`; token inválido, digest do pedido diferente,
forma recusada, instalação já terminada, autorização revogada/superada pelo
supervisor ou supervisor inalcançável ⇒ `400 VerifiedDeployRejected` com a
razão; token de instalação usado em qualquer rota administrativa ⇒
`403 OperationNotPermitted` (só `Deploy`); credencial estranha na rota de
instalações ⇒ `401 VerifiedDeployUnauthorized`.

## Como construir e conferir

Mesmo `self-hosted/build-candidate7.sh` (lê `patchCommit`, `cliCommit` e
`protocol` do manifesto). Hash do patch reproduzível:

```bash
git diff --binary --no-ext-diff --no-color --src-prefix=a/ --dst-prefix=b/ \
  --abbrev=9 849a6fc605b5a910c9834b61a77c5475cc24d189 \
  d46388dc20978fb89935a11727a1d716cabacb70 \
  -- crates Cargo.toml Cargo.lock self-hosted/docker-build rust-toolchain .cargo \
  | sha256sum
```

Resultado: `8d37a6040498768ae3b8a935acda2669ab07828645176ff3dd074defd9e6669b`
(label `org.amage.patch-sha256` e sufixo da versão).

## Estado

Imagem local, sem publicação, sem entrada no catálogo do Synapse. O uso real
registrado é o do laboratório Bendvex (`tools/c7_e2e.py` e
`tests/c7_bypass.py` contra esta tag). A primeira imagem C7
(`…-cfbb0ffd9312`, protocolo v1) continua existindo apenas como histórico;
CLIs v1 são recusadas por este backend com `VerifiedDeployRequired`.

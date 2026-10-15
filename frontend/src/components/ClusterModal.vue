<script setup lang="ts">
// T042 new/edit cluster modal: form maps 1:1 onto ClusterConfig (camelCase);
// unfilled fields stay at serde defaults. auth/ssl are only sent when the
// selected securityProtocol uses them (kafka-core cluster.rs validate()).
import { computed, reactive } from 'vue';
import { clusters, saveCluster, type ClusterRow } from '../store/clusters';

const props = defineProps<{ initial: ClusterRow | null }>();
const emit = defineEmits<{ (e: 'close'): void }>();

const f = reactive({
  name: props.initial?.name ?? '',
  bootstrap: props.initial?.bootstrapServers.join(', ') ?? '',
  mock: props.initial?.mock ?? false,
  clientId: props.initial?.clientId ?? 'rs-kafka-ui-client',
  protocol: props.initial?.securityProtocol ?? 'plaintext',
  mech: props.initial?.auth?.mechanism ?? 'plain',
  user: props.initial?.auth?.username ?? '',
  pass: props.initial?.auth?.password ?? '',
  saslProto: props.initial?.auth?.saslProtocol ?? '',
  caPem: props.initial?.ssl?.caPem ?? '',
  caPath: props.initial?.ssl?.caPath ?? '',
  certPem: props.initial?.ssl?.certPem ?? '',
  keyPem: props.initial?.ssl?.keyPem ?? '',
  verifyCert: props.initial?.ssl?.verifyCertificate ?? true,
  verifyHost: props.initial?.ssl?.verifyHostname ?? true,
});

// serde snake_case wire values from kafka-core cluster.rs.
const PROTOCOLS: [string, string][] = [['plaintext', 'Plaintext'], ['ssl', 'SSL'], ['sasl_plaintext', 'SASL_PLAINTEXT'], ['sasl_ssl', 'SASL_SSL']];
const MECHS = ['plain', 'scram_sha256', 'scram_sha512', 'kafka', 'gss_api', 'oauthbearer'];

const usesSasl = computed(() => f.protocol === 'sasl_plaintext' || f.protocol === 'sasl_ssl');
const usesTls = computed(() => f.protocol === 'ssl' || f.protocol === 'sasl_ssl');

const inp = 'mt-1 w-full rounded border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-800 dark:border-slate-600 dark:bg-slate-900 dark:text-slate-100';

async function submit(): Promise<void> {
  const cfg: Record<string, unknown> = { name: f.name, bootstrapServers: f.bootstrap.split(',').map((s) => s.trim()).filter(Boolean), mock: f.mock, clientId: f.clientId, securityProtocol: f.protocol };
  if (props.initial) cfg.id = props.initial.id;
  if (usesSasl.value) cfg.auth = { mechanism: f.mech, username: f.user, password: f.pass, saslProtocol: f.saslProto || null };
  if (usesTls.value) cfg.ssl = { caPem: f.caPem || null, caPath: f.caPath || null, certPem: f.certPem || null, keyPem: f.keyPem || null, verifyCertificate: f.verifyCert, verifyHostname: f.verifyHost };
  if (await saveCluster(cfg)) emit('close');
}
</script>

<template>
  <div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4" @click.self="emit('close')">
    <form class="max-h-full w-full max-w-lg overflow-auto rounded-lg bg-white p-4 shadow-xl dark:bg-slate-800" @submit.prevent="submit">
      <div class="mb-3 flex items-center justify-between">
        <h3 class="text-base font-semibold text-slate-800 dark:text-slate-100">{{ initial ? '编辑集群' : '新建集群' }}</h3>
        <button type="button" class="text-slate-400 hover:text-slate-600" @click="emit('close')">✕</button>
      </div>
      <p v-if="clusters.error" class="mb-2 rounded bg-rose-100 px-3 py-2 text-xs text-rose-800 dark:bg-rose-900 dark:text-rose-200">{{ clusters.error }}</p>
      <div class="grid grid-cols-2 gap-x-3 gap-y-2 text-xs text-slate-700 dark:text-slate-200">
        <label class="col-span-2 block">名称<input v-model="f.name" required :class="inp" /></label>
        <label class="col-span-2 block">Bootstrap（逗号分隔）<input v-model="f.bootstrap" required :class="inp" placeholder="localhost:9092, host2:9092" /></label>
        <label class="flex items-center gap-2">Mock 驱动<input type="checkbox" v-model="f.mock" /></label>
        <label class="block">Client id<input v-model="f.clientId" :class="inp" /></label>
        <label class="col-span-2 block">安全协议
          <select v-model="f.protocol" :class="inp"><option v-for="[v, l] in PROTOCOLS" :key="v" :value="v">{{ l }}</option></select>
        </label>
        <template v-if="usesSasl">
          <p class="col-span-2 mt-1 font-semibold">SASL</p>
          <label class="block">Mechanism<select v-model="f.mech" :class="inp"><option v-for="m in MECHS" :key="m" :value="m">{{ m }}</option></select></label>
          <label class="block">用户名<input v-model="f.user" :class="inp" /></label>
          <label class="block">密码<input v-model="f.pass" type="password" :class="inp" /></label>
          <label class="block">saslProtocol（可选）<input v-model="f.saslProto" :class="inp" /></label>
        </template>
        <template v-if="usesTls">
          <p class="col-span-2 mt-1 font-semibold">SSL</p>
          <label class="block">CA PEM<textarea v-model="f.caPem" rows="2" :class="inp"></textarea></label>
          <label class="block">CA path<input v-model="f.caPath" :class="inp" /></label>
          <label class="block">Cert PEM<textarea v-model="f.certPem" rows="2" :class="inp"></textarea></label>
          <label class="block">Key PEM<textarea v-model="f.keyPem" rows="2" :class="inp"></textarea></label>
          <label class="flex items-center gap-2">校验证书<input type="checkbox" v-model="f.verifyCert" /></label>
          <label class="flex items-center gap-2">校验主机名<input type="checkbox" v-model="f.verifyHost" /></label>
        </template>
      </div>
      <div class="mt-4 flex justify-end gap-2">
        <button type="button" class="rounded border border-slate-300 px-3 py-1.5 text-xs text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700" @click="emit('close')">取消</button>
        <button type="submit" class="rounded bg-emerald-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-emerald-700">保存</button>
      </div>
    </form>
  </div>
</template>

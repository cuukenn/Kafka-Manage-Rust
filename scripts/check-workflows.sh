#!/usr/bin/env bash
# T130: .github/ 的离线门禁（不需要网络、不需要额外依赖，CI 里也能跑）。
#
# 为什么要有这个脚本：GitHub 的 workflow schema 比「YAML 能不能解析」严格得多。
#   * permissions 只认固定的 scope 列表（schema 里是 additionalProperties:false）。
#     `releases` 不在列表里 —— T128 写了 `releases: write`，于是整份 release.yml 被 GitHub 拒收。
#     症状极其误导：每次 push 生成一个 0 个 job、瞬间失败、名字显示成文件路径（而不是 name:）的 run，
#     tag 触发的构建一次都没跑过，而 API 里没有任何错误文本。
#   * 同一层重复的 key 也会被拒收；ruby 的 YAML.load 只会静默去重，所以得自己查。
#
# 想跑完整校验（一次性、需要联网）：
#   curl -sL https://json.schemastore.org/github-workflow.json -o /tmp/ghwf.json
#   再用 jsonschema 校验 ruby 转出来的 JSON（注意 YAML 1.1 会把 `on:` 读成 true，要改回 "on"）。
set -uo pipefail
cd "$(dirname "$0")/.."

ruby -ryaml -e '
allow = %w[actions artifact-metadata attestations checks code-quality contents deployments
           discussions id-token issues models packages pages pull-requests repository-projects
           security-events statuses vulnerability-alerts copilot-requests]
bad = []
ARGV.each do |f|
  begin
    d = YAML.load_file(f)
  rescue => e
    bad << "#{f}: YAML 解析失败 #{e.message.lines.first}"
    next
  end
  d["on"] = d.delete(true) if d.key?(true)
  walk = lambda do |o|
    case o
    when Hash
      o.each do |k, v|
        if k.to_s == "permissions" && v.is_a?(Hash)
          v.each_key { |s| bad << "#{f}: permissions scope 非法 -> #{s}" unless allow.include?(s.to_s) }
        end
        walk.call(v)
      end
    when Array
      o.each { |v| walk.call(v) }
    end
  end
  walk.call(d)
  chk = lambda do |n|
    if n.is_a?(Psych::Nodes::Mapping)
      seen = Hash.new(0)
      n.children.each_slice(2) { |k, _| seen[k.value] += 1 if k.is_a?(Psych::Nodes::Scalar) }
      seen.each { |k, c| bad << "#{f}: 同层重复 key -> #{k}" if c > 1 }
    end
    chk.call(n.children) if n.respond_to?(:children) && n.children
  end
  chk.call(YAML.parse(f))
  puts "#{f}: jobs=#{d["jobs"] ? d["jobs"].keys.join(",") : "-"}"
end
if bad.empty?
  puts "workflows OK"
else
  bad.uniq.each { |b| puts "ERROR #{b}" }
  exit 1
end' .github/workflows/*.yml .github/actions/*/action.yml

#!/usr/bin/env bash
set -e

export GPG_TTY=$(tty)

mkdir .github/deploy
chmod 700 .github/deploy

gpg --batch --yes \
  --passphrase "${GPG_ENCPASS}" \
  --pinentry-mode loopback \
  --output .github/deploy/pubring.gpg \
  --decrypt .github/encrypted/pubring.gpg.gpg

gpg --batch --yes \
  --passphrase "${GPG_ENCPASS}" \
  --pinentry-mode loopback \
  --output .github/deploy/secring.gpg \
  --decrypt .github/encrypted/secring.gpg.gpg

echo "=== Secret keys available ==="
gpg \
  --homedir "${GITHUB_WORKSPACE}/.github/deploy" \
  --list-secret-keys \
  --keyid-format LONG

echo "=== Test signing ==="
echo "test" | gpg \
  --homedir "${GITHUB_WORKSPACE}/.github/deploy" \
  --batch \
  --yes \
  --pinentry-mode loopback \
  --passphrase "${GPG_PASSPHRASE}" \
  --local-user "${GPG_KEYNAME}" \
  --armor \
  --detach-sign

mvn -B deploy \
  -P ossrh \
  -Dmaven.test.skip=true \
  --settings .github/scripts/settings.xml

rm -rf .github/deploy

export GPG_TTY=$(tty)

mkdir .github/deploy
chmod 0400 .github/deploy

gpg --batch --yes --passphrase ${GPG_ENCPASS} --pinentry-mode loopback --output .github/deploy/pubring.gpg --decrypt .github/encrypted/pubring.gpg.gpg
gpg --batch --yes --passphrase ${GPG_ENCPASS} --pinentry-mode loopback --output .github/deploy/secring.gpg --decrypt .github/encrypted/secring.gpg.gpg

mvn -B deploy -P ossrh -Dmaven.test.skip=true --settings ${GITHUB_WORKSPACE}/settings.xml

rm -rf .github/deploy

# DO NOT MODIFY!
# TODO: Check to see if bash profile gets overwritten if a user writes to it
# Users can modify ~/.custom_profile directly for persistent customizations.
if [ -f ~/.custom_profile ]; then
    . ~/.custom_profile
fi

# Custom prompt: include username, hostname, and current directory
PS1='\[\e[0;32m\]\u@\h \[\e[0;36m\]\w\[\e[0m\]$ '

# Alias to show colorized output of common commands
alias ls='ls --color=auto'
alias grep='grep --color=auto'

# Only print the banner for interactive shells, and send it to stderr
# to avoid contaminating stdout in scripted/automated contexts.
case "$-" in
    *i*)
        cat >&2 <<'EOF'

                        Notice to Users
This is a Federal computer system and is the property of the United States
Government. It is for authorized use only. Users (authorized or unauthorized)
have no explicit or implicit expectation of privacy.
Any or all uses of this system and all files on this system may be intercepted,
monitored, recorded, copied, audited, inspected, and disclosed to authorized
site, Department of Energy, and law enforcement personnel, as well as
authorized officials of other agencies, both domestic and foreign. By using
this system, the user consents to such interception, monitoring, recording,
copying, auditing, inspection, and disclosure at the discretion of authorized
site or Department of Energy personnel.
Unauthorized or improper use of this system may result in administrative
disciplinary action and civil and criminal penalties. By continuing to use
this system you indicate your awareness of and consent to these terms and
conditions of use. LOG OFF IMMEDIATELY if you do not agree to the conditions
stated in this warning.
EOF
        ;;
esac

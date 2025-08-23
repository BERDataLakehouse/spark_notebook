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

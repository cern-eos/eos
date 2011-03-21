/*----------------------------------------------------------------------------*/
#include "console/ConsoleMain.hh"
/*----------------------------------------------------------------------------*/

/* Set the client user and group role */
int
com_role (char *arg) {
  XrdOucTokenizer subtokenizer(arg);
  subtokenizer.GetLine();
  user_role = subtokenizer.GetToken();
  group_role = subtokenizer.GetToken();
  if (!silent) 
    printf("=> selected user role ruid=<%s> and group role rgid=<%s>\n", user_role.c_str(), group_role.c_str());

  if (user_role.beginswith("-"))
    goto com_role_usage;

  return (0);
 com_role_usage:
  printf("usage: role <user-role> [<group-role>]                       : select user role <user-role> [and group role <group-role>]\n");
  
  printf("            <user-role> can be a virtual user ID (unsigned int) or a user mapping alias\n");
  printf("            <group-role> can be a virtual group ID (unsigned int) or a group mapping alias\n");
  return (0);
}

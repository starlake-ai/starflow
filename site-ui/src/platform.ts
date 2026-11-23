type NavigatorLike = { platform?: string; userAgentData?: { platform?: string } };

/** The search shortcut as the user's keyboard names it. */
export function searchShortcut(nav: NavigatorLike | undefined = typeof navigator === 'undefined' ? undefined : navigator): string {
  const platform = nav?.userAgentData?.platform || nav?.platform || '';
  return /mac|iphone|ipad/i.test(platform) ? 'Cmd K' : 'Ctrl K';
}
